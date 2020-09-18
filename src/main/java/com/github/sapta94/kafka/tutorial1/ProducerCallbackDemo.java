package com.github.sapta94.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerCallbackDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrapServers = "127.0.0.1:9092";
        final Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++){

            String key = Integer.toString(i);
            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", key,"hello world "+Integer.toString(i));
            logger.info("key = "+key);
            // send data - asynchronous
            producer.send(record, new Callback() {
                //runs every time a record is suceesfully executed or error is thrown
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null){
                        logger.info("New Record Recieved"+"\n"
                                +"Topic Name: "+recordMetadata.topic()+"\n"
                                +"Partition: "+recordMetadata.partition()+"\n"
                                +"Offset: "+recordMetadata.offset());
                    }else{
                        logger.error("Some error occured",e);
                    }
                }
            }).get();
        }


        // flush data
        producer.flush();
        // flush and close producer
        producer.close();

    }
}