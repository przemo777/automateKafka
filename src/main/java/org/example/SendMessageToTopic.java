package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SendMessageToTopic {
    public static void main(String[] args) {

        String brokers = "glider-01.srvs.cloudkafka.com:9094,glider-02.srvs.cloudkafka.com:9094,glider-03.srvs.cloudkafka.com:9094";
        String topic = "qolm0na9-TopicProd";
        String topic2 = "qolm0na9-TopicStage";
        String username = "qolm0na9";
        String password = "PjnP0vEPretUGH6IlCbU6_qgSy13VdRy";


        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);

        //converts objects to bytes
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        //handle auth
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username =\""+username+"\" password= \""+password+"\";");

        KafkaProducer<String, String> myProducer = new KafkaProducer<>(props);

        ProducerRecord<String, String> record = new ProducerRecord<String,String>(topic,"record1");
        ProducerRecord<String, String> recordKV = new ProducerRecord<String,String>(topic,"this is a key","this is a value");
        ProducerRecord<String, String> recordPart = new ProducerRecord<String,String>(topic2, 2,"key2","value2");
        myProducer.send(record);
        myProducer.send(recordKV);
        myProducer.send(recordPart);

        myProducer.flush();
        myProducer.close();






    }
}