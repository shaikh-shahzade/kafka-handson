package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class.getSimpleName());
    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my-app");
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        KafkaConsumer<String , String> consumer = new KafkaConsumer<>(prop);

        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                logger.info("Detected a shutdown...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        });

        try {
            consumer.subscribe(Arrays.asList("kafka-practice"));
            while (true)
            {
                ConsumerRecords<String,String> consumerRecords = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String,String> consumerRecord:consumerRecords) {
                    logger.info("Cosumed record: "+ consumerRecord.key() + consumerRecord.value()
                            + consumerRecord.topic() + consumerRecord.offset() + consumerRecord.partition()
                            +consumerRecord.timestamp());

                }

            }
        }
        catch (WakeupException wakeupException)
        {
            logger.info(wakeupException.getMessage());
        }
        catch (Exception e)
        {
            logger.info(e.getMessage());
        }
        finally {
            consumer.close();
            logger.info("consumer is closed");
        }

    }
}
