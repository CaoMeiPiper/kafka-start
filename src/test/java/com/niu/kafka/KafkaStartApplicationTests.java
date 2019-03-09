package com.niu.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaStartApplicationTests {

    @Test
    public void contextLoads() {
    }

    @Test
    public void testProducer() throws ExecutionException, InterruptedException {
        Properties props = new Properties();

        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建kafka的生产者类
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        System.out.println("producer is created!");

        String topic = "test";
        producer.send(new ProducerRecord<String, String>(topic, "idea-key2", "java-message 1")).get();
        producer.send(new ProducerRecord<String, String>(topic, "idea-key2", "java-message 2")).get();
        producer.send(new ProducerRecord<String, String>(topic, "idea-key2", "java-message 3")).get();

        producer.close();
        System.out.println("producer is closed!");

    }

    @Test
    public void testConsumer() {
        String topic = "test";
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");//用于建立与 kafka 集群连接的 host/port 组。
        props.put("group.id", "testGroup1");// Consumer Group Name
        props.put("enable.auto.commit", "true");// Consumer 的 offset 是否自动提交
        props.put("auto.commit.interval.ms", "1000");// 自动提交 offset 到 zookeeper 的时间间隔，时间是毫秒
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Consumer<String, String> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(), record.offset(), record.key(), record.value());
        }

    }
}
