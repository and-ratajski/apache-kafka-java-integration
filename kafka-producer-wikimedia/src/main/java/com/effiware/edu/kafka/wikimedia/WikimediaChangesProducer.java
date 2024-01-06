package com.effiware.edu.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "127.0.0.1:9092";
        String topic = "wikimedia.recentchange";
        String recentChangeUrl = "https://stream.wikimedia.org/v2/stream/recentchange";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // high throughput configs
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(recentChangeUrl));
        EventSource eventSource = builder.build();

        // runs in a separate thread
        eventSource.start();

        TimeUnit.MINUTES.sleep(10);

    }
}
