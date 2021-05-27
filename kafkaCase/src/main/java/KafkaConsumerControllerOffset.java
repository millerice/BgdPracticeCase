import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Auther icebear
 * @Date 5/27/21
 */

// kafka消费者（手动提交偏移量）
public class KafkaConsumerControllerOffset {
    public static void main(String[] args) {
        // 属性配置
        Properties props = new Properties();
        // kafka集群
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        // 设置消费者组
        props.put("group.id", "controllerOffset");
        // 关闭自动提交，改为手动提交
        props.put("enable.auto.commit", "false");
        // key,value反序列化
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 指定消费的topic
        consumer.subscribe(Arrays.asList("test"));
        // 指定每次手动提交的数量
        final int minBatchSize = 20;
        // 定义一个数组缓冲区
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();
        // 循环消费数据
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                buffer.add(record);
            }
            // 判断是否需要进行手动提交
            if (buffer.size() >= minBatchSize){
                System.out.println("缓冲区共有消息数：" + buffer.size());
                System.out.println("消息已处理");
                // 提交偏移量
                consumer.commitSync();
                // 清空缓存
                buffer.clear();
            }
        }
    }
}
