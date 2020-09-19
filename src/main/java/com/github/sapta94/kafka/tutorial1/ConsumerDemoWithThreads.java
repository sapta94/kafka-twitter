package com.github.sapta94.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class ConsumerDemoWithThreads {
    public static void main(String[] args) {
        new ConsumerDemoWithThreads().run();
    }
    private ConsumerDemoWithThreads(){

    }

    private void run(){
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "java_app_2";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);

        Runnable myConsumerThread = new ConsumerThread(latch,topic, bootstrapServer, groupId);

        Thread myThread = new Thread(myConsumerThread);
        myThread.start();

        //add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Caught Shutdown hook!");
            ((ConsumerThread)myConsumerThread).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("Application exited");
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application is interrupted",e);
        }finally{
            logger.info("Application is closing!");
        }
    }


    public class ConsumerThread implements Runnable{

        private CountDownLatch latch;
        private KafkaConsumer<String , String> consumer;
        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);

        public ConsumerThread(CountDownLatch latch, String topic, String bootstrapServer, String groupId){
            this.latch = latch;
            //create consumer config
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //create consumer
            consumer= new KafkaConsumer<String, String>(properties);

            //subscribe consumer to topic
            consumer.subscribe(Collections.singleton(topic)); //subscribe to a single topic

        }

        @Override
        public void run() {
            try{
                //poll the data
                while(true){
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for(ConsumerRecord record:records){
                        logger.info("Key = "+record.key()+" Value "+record.value());
                        logger.info("Partitions = "+record.partition()+ " Offsets "+record.offset() );
                    }
                }
            }catch(WakeupException e){
                logger.info("Received Shutdown Signal!");
            } finally {
                consumer.close();
                //tells the main we are done with the consumer
                latch.countDown();
            }
        }

        public void shutdown(){
            //the wakeup is a special method to interrupt consumer.poll()
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}
