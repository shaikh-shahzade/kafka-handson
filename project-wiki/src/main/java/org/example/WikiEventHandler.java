package org.example;


import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiEventHandler implements EventHandler {

    KafkaProducer<String,String> producer ;
    String topic;
    private final Logger logger = LoggerFactory.getLogger(WikiEventHandler.class.getSimpleName());

    public WikiEventHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        producer.close();
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {

        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));
        logger.info(messageEvent.getData());
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        logger.error("unbale to get data",t);
    }
}
