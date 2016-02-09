package net.matosiuk.spikedet.model;

import org.apache.spark.util.StatCounter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class Model implements Serializable {
    private Map<String,Double> stations = new HashMap<>();

    public Model() {}

    public boolean isRushHour(int stationId, long timestamp, double point, Function<Long,Byte> splitter) {
        byte tsClass = splitter.apply(timestamp);
        String key = getKey(stationId,tsClass);
        if (stations.containsKey(key)) {
            return (point>stations.get(key));
        } else {
            return false;
        }
    }

    public void add (int stationId, byte tsClass, double k, StatCounter stats) {
        String key = getKey(stationId,tsClass);
        Double threshold = stats.mean() + k * stats.stdev();
        this.stations.put(key,threshold);
    }

    private String getKey(int stationId, byte tsClass) {
        return stationId+"_"+tsClass;
    }

    @Override
    public String toString(){
        String out = "Spike detection model\n";
        for (Map.Entry<String, Double> entry : stations.entrySet()) {
            String key = entry.getKey();
            Double value = entry.getValue();
            out += "\t"+key+" -> "+value+"\n";
        }
        return out;
    }
}
