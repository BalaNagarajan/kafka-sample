package com.javacircle.kafka.first;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String args[]) {
        System.out.println("-------------Print -------");

        //Create Producer properties...starts
        String kafkaServer = "127.0.0.1:9092";
        Properties propertiesObj = new Properties();
        propertiesObj.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        //Kafka will converts into bytes
        propertiesObj.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propertiesObj.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //Create Producer properties...ends

        //Create the Producer....starts
        KafkaProducer<String,String> kafkaProducerObj = new KafkaProducer<String, String>(propertiesObj);
        //Create the Producer....ends

        //Creates the producer record...starts
        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("first_topic","hello world");
        kafkaProducerObj.send(producerRecord);
        //This is a async call , so we need to flush the data
        kafkaProducerObj.flush();
        kafkaProducerObj.close();

        //Creates the producer record...ends
    }
}
