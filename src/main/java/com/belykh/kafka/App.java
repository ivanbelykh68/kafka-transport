package com.belykh.kafka;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Hello world!
 *
 */
@Slf4j
public class App 
{
    public static void main( String[] args ) throws Exception {
        Properties props = new Properties();
        props.load(App.class.getClassLoader().getResourceAsStream("transport.properties"));
        log.info("Loaded properties: {}", props);
        var producer = new CoordinatesProducer(props);
        var consumer = new CoordinatesConsumer(props);

        CompletableFuture producerFuture = CompletableFuture.runAsync(() -> {
            try {
                producer.generateCoordinates();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        CompletableFuture consumerFuture = CompletableFuture.runAsync(() -> consumer.getCoordinates());

        log.info("Producer and consumer created");

        CompletableFuture.allOf(producerFuture, consumerFuture).join();

        log.info("Finishing application");

        producer.close();
        consumer.close();
    }
}
