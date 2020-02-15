package hello;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class SingleApplication {
    public static void main(String[] args) {


        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.172:9093");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("auto.offset.reset","earliest");

        String topic = "mxw2";

        KafkaConsumer<Long, Integer> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<Long, Integer> records = consumer.poll(1000);
        for (ConsumerRecord<Long, Integer> record : records) {
            System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value(), record.timestamp());
        }
        consumer.close();
    }
}
