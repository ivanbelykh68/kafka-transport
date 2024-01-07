package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class CoordinatesConsumer {

    private final String topic;
    private final int valuesCount;
    private final int sleepDelay;
    private KafkaConsumer<String, Coordinate> kafkaConsumer;

    private int receivedValuesCount = 0;
    public CoordinatesConsumer(Properties props) {
        topic = props.getProperty("coord.topic");
        valuesCount = Integer.valueOf(props.getProperty("kafka-transport.values.count"));
        sleepDelay = Integer.valueOf(props.getProperty("kafka-transport.consumer.sleep.delay"));
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topic));
    }

    public void showCoordinate(Coordinate coordinate) {
        log.info("Got coordinate from Kafka: {}", coordinate);
    }

    public void getCoordinates() {
        while (receivedValuesCount < valuesCount) {
            log.info("Getting coordinates from Kafka");
            ConsumerRecords<String, Coordinate> coordinates = kafkaConsumer.poll(Duration.ofMillis(sleepDelay));
            coordinates.forEach(r -> {receivedValuesCount ++; showCoordinate(r.value());});
        }
    }
}
