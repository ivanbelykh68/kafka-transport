package com.belykh.kafka.partitioner;

import com.belykh.kafka.avro.Coordinate;
import io.confluent.common.utils.Utils;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomCoordinatePartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitionCount = cluster.partitionCountForTopic(topic);
        int calculatedPartition;
        if(value instanceof Coordinate coordinate) {
            calculatedPartition = coordinate.getLat() == 37.2431 && coordinate.getLon() == 115.793
                    ? 5
                    : Math.abs(Utils.murmur2(valueBytes) % partitionCount);
        }
        else {
            calculatedPartition = Math.abs(Utils.murmur2(valueBytes) % partitionCount);
        }

        return calculatedPartition >= partitionCount ? 0 : calculatedPartition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
