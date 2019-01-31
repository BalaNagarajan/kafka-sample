package com.javacircle.kafka.first;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Properties;

public class ProducerCallbackDemo {
    public static void main(String args[]) {
        System.out.println("-------------Print -------");
        Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);
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
        kafkaProducerObj.send(producerRecord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time when the record sent successfully sent to Kafka or exception is thrown

                if(e!=null){
                    logger.info("Received metadata info : Topic"+recordMetadata.topic()+"");

                }
            }
        });
        //This is a async call , so we need to flush the data
        kafkaProducerObj.flush();
        kafkaProducerObj.close();

        //Creates the producer record...ends
    }
}
