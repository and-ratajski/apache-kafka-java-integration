package com.effiware.edu.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());


    public static void main(String[] args) {
        log.info("I am a Kafka Consumer");
        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // none/earliest/latest
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));


        try {
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
//                log.info("Polling...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
 
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("Consumer is starting the shutdown...");
        } catch (Exception e) {
            log.error("Unexpected exception occurred: ", e);
        } finally {
            consumer.close();
            log.info("Consumer was gracefully shut down.");
        }


    }
}
