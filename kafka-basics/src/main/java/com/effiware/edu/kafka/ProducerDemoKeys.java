package com.effiware.edu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());


    public static void main(String[] args) {
        log.info("I am a Kafka Producer With Callback");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "demo_java";
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 30; j++) {
                String key = "id_" + j;
                String value = "hello world with key, j: + " + j;
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                        topic,
                        key,
                        value
                );

                // send data -- asynchronous
                producer.send(producerRecord, (metadata, e) -> {
                    // executes after record sent
                    if (e == null) {
                        log.info("Key: " + key + " | " + "Partition: " + metadata.partition());
                    } else {
                        log.error("Error while producing");
                    }
                });
            }
        }

        // send all data and block until done -- synchronous
        producer.flush();
        producer.close();
    }
}
