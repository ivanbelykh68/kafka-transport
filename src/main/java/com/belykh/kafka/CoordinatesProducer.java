package com.belykh.kafka;

import com.belykh.kafka.avro.Coordinate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.Closeable;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;

@Slf4j
public class CoordinatesProducer implements Closeable {
    private final String topicName;
    private final int numPartitions;
    private final short replicationFactor;
    private final int valuesCount;
    private final int sleepDelay;
    private final KafkaProducer<String, Coordinate> kafkaProducer;
    private int sentValuesCount = 0;
    public CoordinatesProducer(Properties props) {
        topicName = props.getProperty("coord.topic");
        numPartitions = Integer.parseInt(props.getProperty("coord.topic.numPartitions"));
        replicationFactor = Short.parseShort(props.getProperty("coord.topic.replicationFactor"));
        valuesCount = Integer.parseInt(props.getProperty("kafka-transport.values.count"));
        sleepDelay = Integer.parseInt(props.getProperty("kafka-transport.producer.sleep.delay"));
        kafkaProducer = new KafkaProducer<>(props);

        log.info("Creating topic {} with numPartitions={} and replicationFactor={}", topicName, numPartitions, replicationFactor);
        AdminClient adminClient = AdminClient.create(props);
        var topicCreationResult = adminClient.createTopics(List.of(new NewTopic(topicName, numPartitions, replicationFactor)));
        log.info("Topic creation result: {}", topicCreationResult);
    }

    public void sendCoordinate(String topic, Coordinate coordinate) {
        log.info("Sending coordinate to Kafka: {}", coordinate);
        ProducerRecord<String, Coordinate> record = new ProducerRecord<>(topic, "key" + coordinate.getId(), coordinate);
        kafkaProducer.send(record);
    }

    public void generateCoordinates() throws InterruptedException {
        try (kafkaProducer) {
            while (sentValuesCount < valuesCount) {
                Coordinate coordinate = generateCoordinate(sentValuesCount);
                sendCoordinate(topicName, coordinate);
                sentValuesCount ++;
                Thread.sleep(sleepDelay);
            }
        }
    }

    private Coordinate generateCoordinate(int id) {
        boolean isVip = id == 7;
        double lat = isVip ? 37.2431 : Math.random() * 180 - 90;
        double lon = isVip ? 115.793 : Math.random() * 360 - 180;
        double speed = 1000 + id * 100 + ((double)id)/100;
        return new Coordinate(sentValuesCount,lat, lon, speed, LocalDateTime.now());
    }

    @Override
    public void close() throws IOException {
        kafkaProducer.close();
    }
}
