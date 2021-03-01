package app;

import com.alibaba.fastjson.JSON;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import utils.MyKafkaUtil;

import java.beans.SimpleBeanInfo;
import java.io.OutputStream;
import java.security.KeyFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ClassName: BaseLogApp
 * Author: hsz
 * Date: 2021-02-28 22:37
 * Description: //从kafka中读取ods层用户行为日志数据，分为启动日志，页面日志，曝光日志
 **/
public class BaseLogApp {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_PAGE = "dwd_page_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    
    public static void main(String[] args) throws Exception {
        //TODO 0 基本环境准备
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度，这里和kafka分区保持一致
        env.setParallelism(4);
        //设置ck相关参数
        //设置精准一次性保证(默认) 每5000ms开始一次checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //Checkpoint必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall/flink/checkpoint"));
        System.setProperty("HADOOP_USER_NAME", "zuoge");
        
        //指定消费者配置信息
        String groupId = "ods_dwd_base_log_app";
        String topic = "ods_base_log";
        
        //TODO 1 从kafka中读数据
        //调用kafka工具类，从指定kafka主题读取数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //转换为json对象
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                return jsonObject;
            }
        });

        /*//打印测试
        jsonObjectDS.print();*/
        
        //TODO 2 识别新老访客
        //按照mid进行分组
        KeyedStream<JSONObject, String> midKeyedStream = jsonObjectDS.keyBy(
                data -> data.getJSONObject("common").getString("mid"));

        //校验收集到的数据是新老访客
        SingleOutputStreamOperator<JSONObject> midWithNewFlagDS = midKeyedStream.map(
                new RichMapFunction<JSONObject, JSONObject>() {
            //声明第一次访问日期的状态
            private ValueState<String> firstVisitDataState;
            //声明日期数据格式化对象
            private SimpleDateFormat simpleDateFormat;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化数据
                firstVisitDataState = getRuntimeContext().getState(
                        new ValueStateDescriptor<String>("newMidDateState", String.class));
                simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {
                //打印数据
                System.out.println(jsonObj);
                //获取访问标记 0表示老访客 1表示新访客
                String isNew = jsonObj.getJSONObject("common").getString("is_new");
                //获取数据中的时间戳
                Long ts = jsonObj.getLong("ts");

                //判断标记如果为1，则继续校验数据
                if ("1".equals(isNew)) {
                    //获取新访客状态
                    String newMidDate = firstVisitDataState.value();
                    //获取当前数据访问日期
                    String tsDate = simpleDateFormat.format(new Date(ts));

                    //如果新访客状态不为空，说明该设备已访问过，则将访问标记置为0
                    if (newMidDate != null && newMidDate.length() != 0) {
                        if (!newMidDate.equals(tsDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    } else {
                        //如果复检后，该设备的确没有访问过，那么更新状态为当前日期
                        firstVisitDataState.update(tsDate);
                    }
                }
                //返回确认过新老访客的json数据
                return jsonObj;
            }
        });
        
        
        //TODO 3 利用测输出流实现数据拆分
        //定义启动和曝光数据的测输出流标签
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        //页面日志，启动日志，曝光日志
        //将不同的日志输出导不同的流中
        SingleOutputStreamOperator<String> pageDStream = midWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObj, Context context, Collector<String> collector) throws Exception {
                //获取数据中的启动相关字段
                JSONObject startJsonObj = jsonObj.getJSONObject("start");
                //将数据转换为字符串，准备向流中输出
                String dataStr = jsonObj.toString();
                //如果是启动日志，输出到启动侧输出流
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    context.output(startTag, dataStr);
                } else {
                    //非启动日志，则是页面日志或者曝光日志
                    System.out.println("PageString" + dataStr);
                    //获取数据中的曝光数据，如果不为空，则将每条曝光数据取出输出到曝光日志测输出流
                    JSONArray displays = jsonObj.getJSONArray("display");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            //获取页面Id
                            String pageId = jsonObj.getJSONObject("page").getString("page_id");
                            //给每条曝光信息添加上pageId
                            displayJsonObj.put("page_id", pageId);
                            //将曝光数据输出到测输出流
                            context.output(displayTag, displayJsonObj.toString());
                        }
                    } else {
                        //将页面数据输出到主流
                        collector.collect(dataStr);
                    }
                }
            }
        });

        //获取测输出流
        DataStream<String> startDStream = pageDStream.getSideOutput(startTag);
        DataStream<String> displayDStream = pageDStream.getSideOutput(displayTag);
        
        
        //TODO 4 将数据输出到kafka不同主题中
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        
        startDStream.addSink(startSink);
        pageDStream.addSink(pageSink);
        displayDStream.addSink(displaySink);
        
        //执行
        env.execute("dwd_base_log job");
    }
}
