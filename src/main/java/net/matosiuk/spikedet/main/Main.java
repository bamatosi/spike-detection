package net.matosiuk.spikedet.main;

import net.matosiuk.spikedet.model.DataPoint;
import net.matosiuk.spikedet.model.Model;
import net.matosiuk.spikedet.splitter.WeekdayWeekendSplitter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;

import java.util.*;
import java.util.function.Function;

public class Main {
    public static void main(String [] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setAppName("RushHours").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /* BUILD MODEL */
        /* Params */
        double k = 0.5; // Standard deviation multiplier i.e. how much the time series values can be away from the mean
        Function<Long,Byte> splitter = WeekdayWeekendSplitter.getSplitter(); // Time series splitter, this one is splitting by the time (weekdays and weekends are analyzed separately)
        String dataPath = "./target/classes";

        /* Preparing the data */
        String file = dataPath+"/multiple_wd_ts.csv";

        JavaPairRDD<Integer,DataPoint> data = sc.textFile(file)
                .mapToPair((String s) -> {
                    String line[] = s.split(",");
                    long timestamp = Long.parseLong(line[2]);
                    double point = Double.parseDouble(line[3]);
                    return new Tuple2<Integer, DataPoint>(Integer.parseInt(line[1]), new DataPoint(timestamp, point, splitter));
                })
                .cache();

        List<Tuple2<Integer,Byte>> stations = data.mapToPair(t->new Tuple2<Integer,Byte>(t._1(),t._2().getTsClass())).distinct().collect();

        /* Building model */
        Model model = new Model();
        for (Tuple2<Integer,Byte> tuple : stations) {
            int stationId = tuple._1();
            byte tsClass = tuple._2();
            JavaDoubleRDD ts = data.filter(t->((t._1()==stationId) && t._2().getTsClass()==tsClass)).mapToDouble(t -> t._2().getPoint());
            StatCounter stats = ts.stats();
            model.add(stationId,tsClass,k,stats);
        }
        System.out.println(model.toString());
        Broadcast<Model> modelSh = sc.broadcast(model);

        /* TEST MODEL */
        /* Use sa model to mark rush hours for station 1 */
        String filetest =  dataPath+"/st1-test.csv";
        sc.textFile(filetest)
            .mapToPair((String s) -> {
                String line[] = s.split(",");
                int stationId = 1;
                long timestamp = Long.parseLong(line[1]);
                double point = Double.parseDouble(line[2]);
                int rushhour = 0;
                if (modelSh.getValue().isRushHour(stationId, timestamp, point, splitter)) {
                    rushhour = 1;
                }
                return new Tuple2<Integer, Integer>(Integer.parseInt(line[1]), rushhour);
            })
            .saveAsTextFile( dataPath+"/st1-analysis");

        /* Stop Spark */
        sc.stop();
    }
}