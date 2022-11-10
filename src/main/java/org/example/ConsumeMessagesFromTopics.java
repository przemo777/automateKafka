package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Arrays;
import java.util.Properties;

public class ConsumeMessagesFromTopics {

    public static void main(String[] args) {


        String brokers = "glider-01.srvs.cloudkafka.com:9094,glider-02.srvs.cloudkafka.com:9094,glider-03.srvs.cloudkafka.com:9094";
        String topic = "qolm0na9-TopicProd";
      //  String topic2 = "qolm0na9-TopicStage";
        String username = "qolm0na9";
        String password = "PjnP0vEPretUGH6IlCbU6_qgSy13VdRy";

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", brokers);

        //converts bytes to object
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());

        //handle auth
        props.setProperty("security.protocol", "SASL_SSL");
        props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        props.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username =\""+username+"\" password= \""+password+"\";");

        props.setProperty("group.id","myconsumer");


        //create consumer obj using kafka consumer class
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //from which topic to consume

        consumer.subscribe(Arrays.asList(topic));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100);

            for (ConsumerRecord<String,String> record:records) {
                System.out.println(record.value());
            }

        }






    }
}
