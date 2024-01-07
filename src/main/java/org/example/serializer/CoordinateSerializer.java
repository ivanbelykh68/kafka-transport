package org.example.serializer;

import org.example.Coordinate;

public class CoordinateSerializer extends JsonSerializer<Coordinate> {
    public CoordinateSerializer() {
        super(Coordinate.class);
    }
}
