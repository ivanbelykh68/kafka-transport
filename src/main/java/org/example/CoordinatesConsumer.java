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
    //private final int valuesCount;
    private final int sleepDelay;
    private final int workTime;
    private KafkaConsumer<String, Coordinate> kafkaConsumer;

    private int receivedValuesCount = 0;
    public CoordinatesConsumer(Properties props) {
        topic = props.getProperty("coord.topic");
        //valuesCount = Integer.valueOf(props.getProperty("kafka-transport.values.count"));
        sleepDelay = Integer.valueOf(props.getProperty("kafka-transport.consumer.sleep.delay"));
        workTime = Integer.valueOf(props.getProperty("kafka-transport.consumer.work.time"));
        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(List.of(topic));
    }

    private void showCoordinate(Coordinate coordinate) {
        log.info("Got {} coordinate from Kafka: {}", receivedValuesCount, coordinate);
    }

    public void getCoordinates() {
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() < startTime + workTime) {
            log.info("Getting coordinates from Kafka");
            ConsumerRecords<String, Coordinate> coordinates = kafkaConsumer.poll(Duration.ofMillis(sleepDelay));
            coordinates.forEach(r -> {receivedValuesCount ++; showCoordinate(r.value());});
        }
    }
}
