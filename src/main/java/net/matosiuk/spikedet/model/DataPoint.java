package net.matosiuk.spikedet.model;

import java.util.function.Function;

public class DataPoint {
    private byte tsClass; // point's class for example 1-weekday, 2-weekend
    private double point; // point's value

    public DataPoint(long timeStamp, double point, Function<Long,Byte> splitter) {
        this.tsClass = splitter.apply(timeStamp);
        this.point = point;
    }

    public byte getTsClass() {
        return tsClass;
    }

    public double getPoint() {
        return point;
    }
}
