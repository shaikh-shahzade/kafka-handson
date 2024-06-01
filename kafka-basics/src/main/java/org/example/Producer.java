package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class Producer {
    private static  final Logger logger= LoggerFactory.getLogger(Producer.class.getSimpleName());
    public static void main(String[] args) {

        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(prop);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("kafka-practice", "key1", "Hello kafka");

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                logger.info("topic"+metadata.topic()
                        + "partition"+metadata.partition()
                        + "offset"+metadata.offset()
                        + "time" +metadata.timestamp());

            }
        });

        kafkaProducer.flush();

        kafkaProducer.close();

    }
}
