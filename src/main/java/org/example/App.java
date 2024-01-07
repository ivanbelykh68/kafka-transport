package org.example;

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
        System.out.println("Loaded properties: " + props);
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

        System.out.println("Producer and consumer created");

        CompletableFuture.allOf(producerFuture, consumerFuture).join();

        System.out.println("Finishing application");
    }
}
