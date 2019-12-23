package ru.goncharov.traffic_counter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import static ru.goncharov.traffic_counter.Constant.ALERT_TOPIC;
import static ru.goncharov.traffic_counter.Constant.PRODUCER_ADDRESS;

public class Producer implements Closeable {

    // Kafka producer for send messages to kafka topic
    private KafkaProducer<String, String> producer;

    public Producer() {
        init();
    }

    // Kafka producer initialization with properties
    private void init() {
        Properties props = new Properties();
        props.put("bootstrap.servers", PRODUCER_ADDRESS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    // Send message to kafka topic
    public void sendAlertTopic(String message) {
        ProducerRecord<String, String> record = new ProducerRecord<>(ALERT_TOPIC, message);
        producer.send(record);
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }
}
