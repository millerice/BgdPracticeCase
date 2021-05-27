import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @Auther icebear
 * @Date 5/27/21
 */

// kafka消费者代码（自动提交偏移量）
public class KafkaConsumerStudy {
    public static void main(String[] args) {
        // 配置属性
        Properties props = new Properties();
        // Kafka集群地址
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        // 消费者组id
        props.put("group.id", "test");
        // 自动提交偏移量
        props.put("enable.auto.commit", "true");
        // 自动提交偏移量的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // 设置消费方式 默认是latest
        props.put("auto.offset.reset", "earliest");
        // 反序列化key value
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 指定消费者消费哪些topit
        consumer.subscribe(Arrays.asList("test"));
        while (true){
            // 指定多久拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
        // 关闭资源

    }
}
