package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class CoordinatesProducer {
    private final String topic;
    private final int valuesCount;
    private final int sleepDelay;
    private KafkaProducer<String, Coordinate> kafkaProducer;
    private int sentValuesCount = 0;
    public CoordinatesProducer(Properties props) {
        topic = props.getProperty("coord.topic");
        valuesCount = Integer.valueOf(props.getProperty("kafka-transport.values.count"));
        sleepDelay = Integer.valueOf(props.getProperty("kafka-transport.producer.sleep.delay"));
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void sendCoordinate(String topic, Coordinate coordinate) {
        log.info("Sending coordinate to Kafka: {}", coordinate);
        ProducerRecord<String, Coordinate> record = new ProducerRecord<>(topic, coordinate);
        kafkaProducer.send(record);
    }

    public void generateCoordinates() throws InterruptedException {
        while (sentValuesCount < valuesCount) {
            Coordinate coordinate = new Coordinate(Math.random() * 180 - 90, Math.random() * 360 - 180);
            sendCoordinate(topic, coordinate);
            sentValuesCount ++;
            Thread.sleep(sleepDelay);
        }
    }
}
