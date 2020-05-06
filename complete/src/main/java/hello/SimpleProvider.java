package hello;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.Random;

@RestController
public class SimpleProvider {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.10.172:9093");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        String topic = "mxw2";
        long key;

        Random random = new Random();
        KafkaProducer<Long, Integer> kafkaProducer = new KafkaProducer<>(properties);

        for(int n = 0; n < 10000; n++){
            for (int i = 1; i <= 1000; i++) {
                Integer value = random.nextInt(100);
                key = System.currentTimeMillis();
                kafkaProducer.send(new ProducerRecord<>(topic, key, value));
                Thread.sleep(500);
            }
            Thread.sleep(1000);
        }
        kafkaProducer.close();
    }

    @RequestMapping("/provide")
    public String provide() throws InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.10.172:9093");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        String topic = "mxw2";
        long key;

        Random random = new Random();
        KafkaProducer<Long, Integer> kafkaProducer = new KafkaProducer<>(properties);

        for(int n = 0; n < 1; n++){
            for (int i = 1; i <= 1000; i++) {
                Integer value = random.nextInt(100);
                key = System.currentTimeMillis();
                kafkaProducer.send(new ProducerRecord<>(topic, key, value));
                Thread.sleep(5);
            }
            Thread.sleep(1000);
        }
        kafkaProducer.close();

        return "provide 1000 data to kafka";
    }

}
