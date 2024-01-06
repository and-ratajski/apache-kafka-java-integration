package com.effiware.edu.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());


    public static void main(String[] args) {
        log.info("I am a Kafka Producer With Callback");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 30; j++) {
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                        "demo_java",
                        "hello world with callback, i: " + i + ", j:" + j
                );

                // send data -- asynchronous
                producer.send(producerRecord, (metadata, e) -> {
                    // executes after record sent
                    if (e == null) {
                        log.info("Received new mentada \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n"
                        );
                    } else {
                        log.error("Error while producing");
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // send all data and block until done -- synchronous
        producer.flush();

        producer.close();
    }
}
