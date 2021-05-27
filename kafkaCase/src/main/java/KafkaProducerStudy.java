import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @Auther icebear
 * @Date 5/27/21
 */

// 开发kafka生产者代码
public class KafkaProducerStudy {
    public static void main(String[] args) {
        // 准备配置属性
        Properties props = new Properties();
        // Kafka集群地址
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        // acks消息确认机制
        props.put("acks", "1");
        // 重试次数
        props.put("retries", 3);
        // 批处理数据大小
        props.put("batch.size", 16384);
        // 延长多久发送数据
        props.put("linger.ms", 100);
        // 缓冲区大小
        props.put("buffer.memory", 33554432);
        // 序列化key value
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 模拟自定义生产数据
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i),
                    "hello kafka, you is number:" + i));
        // 关闭资源
        producer.close();
    }
}
