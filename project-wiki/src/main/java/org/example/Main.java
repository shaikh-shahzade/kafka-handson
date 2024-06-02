package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws InterruptedException {

        String bootstrap = "127.0.0.1:9092";
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrap);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(prop);
        String topic = "wiki-recent";
        EventHandler eventHandler = new WikiEventHandler(kafkaProducer,topic);
        String url = "";
        EventSource.Builder eventBuilder = new EventSource.Builder(eventHandler, URI.create(url));

        EventSource eventSource = eventBuilder.build();

        eventSource.start();

        TimeUnit.MINUTES.sleep(10);

    }
}