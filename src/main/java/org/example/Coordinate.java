package org.example;

public record Coordinate(int id, double lat, double lon) {

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append("ID=");
        result.append(id);
        result.append(",");
        result.append(Math.abs(lat));
        result.append(lat > 0 ? " N," : " S,");
        result.append(Math.abs(lon));
        result.append(lon > 0 ? " E" : " W");
        return result.toString();
    }
}
