package com.flinklearn.realtime.common;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MapCountPrinter {

    //Print the count of records received by a DataStream in a 3-second time window
    public static void printCount(DataStream<Object> dsObj, String message) {

        dsObj
            //Generate a counter: each input record is counted as 1
            .map(i -> new Tuple2<String, Integer>(message, 1))
            .returns(Types.TUPLE(Types.STRING, Types.INT))

            //Use a time window of 3 secs to aggregate the data
            .timeWindowAll(Time.seconds(3))

            //Use Reduce to accumulate the number of records in every 3-second interval
            .reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1))

            //Print the summary
            .map( new MapFunction<Tuple2<String,Integer>, Integer>() {
                    @Override
                    public Integer map(Tuple2<String,Integer> counter) throws Exception {
                        Utils.printHeader(counter.f0 + " : " + counter.f1);
                        return counter.f1;
                    }
            } );

    }

}
