package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
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
        props.put("bootstrap.servers", brokers);
        props.put("transactional.id", "my-transactional-id");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //converts objects to bytes
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        //handle auth
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username =\""+username+"\" password= \""+password+"\";");

        KafkaProducer<String, String> myProducer = new KafkaProducer<>(props);


        myProducer.initTransactions();
        try {
            myProducer.beginTransaction();
            for (int i = 0; i < 100; i++)
                myProducer.send(new ProducerRecord<>(topic, Integer.toString(i), Integer.toString(i)));
            myProducer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
           // System.out.println("exception");
            myProducer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
           // System.out.println("ABORTED");
            myProducer.abortTransaction();
        }
        myProducer.close();

        myProducer.flush();
        myProducer.close();






    }
}