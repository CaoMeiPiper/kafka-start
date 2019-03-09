package com.niu.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by ami on 2019/3/9.
 */
public class MessageProducer {
    private static final String TOPIC = "education-info";
    private static final String BROKER_LIST = "localhost:9092";
    private static KafkaProducer<String, String> producer = null;

    static {
        Properties configs = initConfig();
        //1.首先初始化KafkaProducer对象。
        producer = new KafkaProducer<String, String>(configs);
    }

    private static Properties initConfig() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
//        0，不等服务器响应，直接返回发送成功。速度最快，但是丢了消息是无法知道的
//        1，leader副本收到消息后返回成功
//        all，所有参与的副本都复制完成后返回成功。这样最安全，但是延迟最高。
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public static void main(String[] args) {
        try {
            String message = "hello world";
            //2.创建要发送的消息对象。
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, message);
            //3.通过producer的send方法，发送消息
            producer.send(record, new Callback() {
                //4、发送消息时，可以通过回调函数，取得消息发送的结果。异常发生时，对异常进行处理。
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (null == exception) {
                        System.out.println("perfect!");
                    }
                    if (null != metadata) {
                        System.out.print("offset:" + metadata.offset() + ";partition:" + metadata.partition());
                    }
                }
            }).get();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}

