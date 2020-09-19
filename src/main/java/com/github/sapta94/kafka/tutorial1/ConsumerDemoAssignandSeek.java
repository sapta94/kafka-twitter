package com.github.sapta94.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoAssignandSeek {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "java_app_2";
        String topic = "first_topic";

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer
        KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(properties);

        //assign and seek are used to replay data and fetch a specific message
        //assign
        TopicPartition partionList = new TopicPartition(topic,0);
        long offset = 15L;
        consumer.assign(Arrays.asList(partionList));

        //seek
        consumer.seek(partionList,offset);

        int numberOfMessageToRead = 5;

        //poll the data
        while(numberOfMessageToRead>0){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record:records){
                logger.info("Key = "+record.key()+" Value "+record.value());
                logger.info("Partitions = "+record.partition()+ " Offsets "+record.offset() );
            }
            numberOfMessageToRead--;
        }
        logger.info("Applciation exited");
    }
}
