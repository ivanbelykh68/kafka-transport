package com.belykh.kafka;

import com.belykh.kafka.avro.Coordinate;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

@Slf4j
public class CoordinatesStreamConverter implements Closeable {

    private final String inputTopicName;
    private final String outputTopicName;

    private final KafkaStreams kafkaStreams;
    public CoordinatesStreamConverter(Properties props) {
        fixProperties(props);

        inputTopicName = props.getProperty("coord.topic");
        outputTopicName = props.getProperty("coord.converted.topic");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Coordinate> kStream = streamsBuilder.stream(inputTopicName);

        //For coordinates with odd id multiplying speed x3
        kStream.filter((key, value) -> value.getId() % 2 != 0)
                .map((key, value) -> new KeyValue<>(key, "Speed x3 = " + (3 * value.getSpeed())))
                .to(outputTopicName, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = streamsBuilder.build();
        log.info("Topology: \r\n" + topology.describe());

        kafkaStreams = new KafkaStreams(topology, props);
    }

    public void start() {
        log.info("Starting coordinates converter");
        kafkaStreams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (IOException e) {
                log.error("Error closing coordinates converter", e);
            }
        }));
    }

    private void fixProperties(Properties props) {
        if(props.getProperty("coord.converted.topic") == null)
            props.setProperty("coord.converted.topic", props.getProperty("coord.topic") + ".converted");
        if(props.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) == null)
            props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "coodrinates-converter");
        /*if(props.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG) == null)
            props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        if(props.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG) == null)
            props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());*/
        if(props.getProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG) == null)
            props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, AvroSerde.class.getName());
        if(props.getProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG) == null)
            props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AvroSerde.class.getName());
    }


    @Override
    public void close() throws IOException {
        log.info("Stopping coordinates converter");
        kafkaStreams.close();
    }


    public static class AvroSerde<T> extends Serdes.WrapperSerde<T> {
        public AvroSerde() {
            super((Serializer<T>) new KafkaAvroSerializer(), (Deserializer<T>) new KafkaAvroDeserializer());
        }
    }
}
