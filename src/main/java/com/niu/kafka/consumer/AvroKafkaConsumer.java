package com.niu.kafka.consumer;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * Created by p on 2018/10/14.
 */
public class AvroKafkaConsumer {

    public static final String USER_SCHEMA = "{\n" +
            "    \"type\":\"record\",\n" +
            "    \"name\":\"Customer\",\n" +
            "    \"fields\":[\n" +
            "        {\"name\":\"id\",\"type\":\"int\"},\n" +
            "        {\"name\":\"name\",\"type\":\"string\"},\n" +
            "        {\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":\"null\"}\n" +
            "    ]\n" +
            "}";

    public static void main(String[] args) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "127.0.0.1:9092");

        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        kafkaProps.put("group.id", "DemoAvroKafkaConsumer");

        kafkaProps.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(kafkaProps);

        consumer.subscribe(Collections.singletonList("Customer"));

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);

        try {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(10);
                for (ConsumerRecord<String, byte[]> record : records) {
                    GenericRecord record1 = injection.invert(record.value()).get();
                    System.out.println(record.key() + ":" + record1.get("id") + "\t" + record1.get("name") + "\t" + record1.get("email"));
                }
            }
        } finally {
            consumer.close();
        }
    }
}