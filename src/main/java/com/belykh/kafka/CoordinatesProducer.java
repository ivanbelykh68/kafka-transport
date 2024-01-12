package com.belykh.kafka;

import com.belykh.kafka.avro.Coordinate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;

@Slf4j
public class CoordinatesProducer implements Closeable {
    private final String topic;
    private final int valuesCount;
    private final int sleepDelay;
    private final KafkaProducer<String, Coordinate> kafkaProducer;
    private int sentValuesCount = 0;
    public CoordinatesProducer(Properties props) {
        topic = props.getProperty("coord.topic");
        valuesCount = Integer.parseInt(props.getProperty("kafka-transport.values.count"));
        sleepDelay = Integer.parseInt(props.getProperty("kafka-transport.producer.sleep.delay"));
        kafkaProducer = new KafkaProducer<>(props);
    }

    public void sendCoordinate(String topic, Coordinate coordinate) {
        log.info("Sending coordinate to Kafka: {}", coordinate);
        ProducerRecord<String, Coordinate> record = new ProducerRecord<>(topic, "" + (coordinate.getId() % 2), coordinate);
        kafkaProducer.send(record);
    }

    public void generateCoordinates() throws InterruptedException {
        try (kafkaProducer) {
            while (sentValuesCount < valuesCount) {
                Coordinate coordinate = new Coordinate(sentValuesCount,Math.random() * 180 - 90, Math.random() * 360 - 180, LocalDateTime.now());
                sendCoordinate(topic, coordinate);
                sentValuesCount ++;
                Thread.sleep(sleepDelay);
            }
        }
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }
}
