package com.niu.kafka.producer;

import java.util.Properties;
import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * Created by ami on 2019/3/10.
 */
public class AvroKafkaProducer {
    public static final String USER_SCHEMA = "{\n" +
            "    \"type\":\"record\",\n" +
            "    \"name\":\"Customer\",\n" +
            "    \"fields\":[\n" +
            "        {\"name\":\"id\",\"type\":\"int\"},\n" +
            "        {\"name\":\"name\",\"type\":\"string\"},\n" +
            "        {\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":\"null\"}\n" +
            "    ]\n" +
            "}";


    public static void main(String[] args){

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers","127.0.0.1:9092");
        kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
//        kafkaProps.put("partitioner.class","MyPartitioner");

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(USER_SCHEMA);

        Injection<GenericRecord,byte[]> injection = GenericAvroCodecs.toBinary(schema);
        KafkaProducer producer = new KafkaProducer<String,byte[]>(kafkaProps);
        for(int i = 0;i < 1000;i++){
            GenericData.Record record = new GenericData.Record(schema);
            record.put("id",i);
            record.put("name","name-"+i);
            record.put("email","email-"+i);
            byte[] bytes = injection.apply(record);
            ProducerRecord<String,byte[]> record1 = new ProducerRecord<String, byte[]>("Customer","customer-"+i,bytes);
            producer.send(record1);
        }
        producer.close();
        System.out.println(USER_SCHEMA);
    }
}
