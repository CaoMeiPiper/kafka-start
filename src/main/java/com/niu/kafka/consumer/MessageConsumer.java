package com.niu.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 * Created by ami on 2019/3/9.
 */
public class MessageConsumer {

    private static final String TOPIC = "education-info";
    private static final String BROKER_LIST = "localhost:9092";
    private static KafkaConsumer<String, String> kafkaConsumer = null;

    static {
        Properties properties = initConfig();
        //1、初始化消费者KafkaConsumer，
        kafkaConsumer = new KafkaConsumer<String, String>(properties);
        //2.并订阅主题。
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));

//        kafkaConsumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
//            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
//                再均衡之前和消费者停止读取消息之后调用
//                kafkaConsumer.commitSync(currentOffsets);
//            }
//        });

    }

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "test");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        try {
            while (true) {
//              3.循环拉取消息
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//              4.拉取回消息后，循环处理。
                for (ConsumerRecord record : records) {
                    try {
                        System.out.println(record.value());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                kafkaConsumer.commitSync();
            } finally {
                kafkaConsumer.close();
            }
        }
    }
}

