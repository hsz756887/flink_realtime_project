package utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.Properties;

/**
 * ClassName: MyKafkaUtil
 * Author: hsz
 * Date: 2021-02-28 22:29
 * Description: //操作kafka的工具类
 **/
public class MyKafkaUtil {
    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    
    //封装kafka消费者
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);
    }
    
    //封装kafka生产者(写)
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(kafkaServer, topic, new SimpleStringSchema());
    }
}
