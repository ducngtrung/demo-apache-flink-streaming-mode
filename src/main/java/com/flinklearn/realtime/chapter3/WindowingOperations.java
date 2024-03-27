package com.flinklearn.realtime.chapter3;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/*
A Flink program that reads a Kafka topics stream and perform window operations on Sliding window and Session window.
 */

public class WindowingOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                         Set up Flink environment
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *                  Read Kafka Topic Stream into a DataStream
             ****************************************************************************/

            //Set connection properties to Kafka cluster
            Properties kafkaProps = new Properties();
            kafkaProps.setProperty("group.id", "flinklearn.realtime");

            //Specify brokers list: "host:port, another_host:port,..."
            kafkaProps.setProperty("bootstrap.servers", "10.82.81.128:9092,10.82.81.157:9092");

            //Set SASL authentication properties
            kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.put("sasl.mechanism", "PLAIN");
            kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='...' password='...';");

            //Create a Kafka consumer based on the properties
            FlinkKafkaConsumer<String> kafkaConsumer =
                    new FlinkKafkaConsumer<>(
                            "flink.kafka.streaming.source", //the Kafka topic from which you consume messages
                            new SimpleStringSchema(),   //schema for the incoming data
                            kafkaProps  //connection properties
                    );

            //Let the consumer receive only new messages, we won't be reading any old messages
            kafkaConsumer.setStartFromLatest();

            //Create a DataStream with the Kafka consumer as data source
            DataStream<String> auditTrailStr = streamEnv.addSource(kafkaConsumer);

            //Use Map to convert each record in the DataStream to an AuditTrail object (POJO class)
            DataStream<AuditTrail> auditTrailObj
                    = auditTrailStr
                        .map( (MapFunction<String, AuditTrail>) auditStr -> {
                            System.out.println("--- Received Record : " + auditStr);
                            return new AuditTrail(auditStr);
                        } );
            /* When the Kafka Generator has started sending events to Kafka, the operation code consumes data from Kafka in real time and prints only the latest messages. */

            /****************************************************************************
             *                            Use Sliding window
             ****************************************************************************/

            /* Compute the count of events, minimum timestamp and maximum timestamp for each interval of 10 seconds, sliding by 5 seconds. */
            DataStream<Tuple4<String, Integer, Long, Long>> slidingSummary
                    = auditTrailObj
                        .map(i -> new Tuple4<String, Integer, Long, Long>
                                (String.valueOf(System.currentTimeMillis()), //Current timestamp
                                1,      //Count each record as 1
                                i.getTimestamp(),   //Minimum Timestamp
                                i.getTimestamp()))  //Maximum Timestamp
                        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
                        .timeWindowAll(
                                Time.seconds(10), //Window size
                                Time.seconds(5))  //Slide by 5
                        .reduce((x, y) -> new Tuple4<String, Integer, Long, Long>
                                        (x.f0,                  //Timestamp
                                        x.f1 + y.f1,            //Accumulate the count of records
                                        Math.min(x.f2, y.f2),   //Compare to find the minimum timestamp
                                        Math.max(x.f3, y.f3))); //Compare to find the maximum timestamp

            //Pretty print
            slidingSummary
                    .map( new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
                        @Override
                        public Object map(Tuple4<String, Integer, Long, Long> slidingSummary) throws Exception {
                            SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
                            String minTime = format.format(new Date(slidingSummary.f2));
                            String maxTime = format.format(new Date(slidingSummary.f3));
                            System.out.println("--- Sliding Summary: " + (new Date()).toString()
                                + " - Start Time: " + minTime
                                + " - End Time: " + maxTime
                                + " - Count: " + slidingSummary.f1);
                            return null;
                        }
                    } );

//            /****************************************************************************
//             *                            Use Session window
//             ****************************************************************************/
//
//            //Execute the same example as before using Session windows
//            //Partition by User and use a window gap of 5 seconds.
//            DataStream<Tuple4<String, Integer, Long, Long>>
//                    sessionSummary
//                    = auditTrailObj
//                    .map(i
//                            -> new Tuple4<String, Integer, Long, Long>
//                            (i.getUser(), //Get user
//                                    1,      //Count each Record
//                                    i.getTimestamp(),   //Minimum Timestamp
//                                    i.getTimestamp()))  //Maximum Timestamp
//                    .returns(Types.TUPLE(Types.STRING,
//                            Types.INT,
//                            Types.LONG,
//                            Types.LONG))
//
//                    .keyBy(0) //Key by user
//
//                    .window(ProcessingTimeSessionWindows
//                            .withGap(Time.seconds(5)))
//
//                    .reduce((x, y)
//                            -> new Tuple4<String, Integer, Long, Long>(
//                            x.f0,
//                            x.f1 + y.f1,
//                            Math.min(x.f2, y.f2),
//                            Math.max(x.f3, y.f3)));
//
//            //Pretty print
//            sessionSummary.map(new MapFunction<Tuple4<String, Integer,
//                    Long, Long>, Object>() {
//
//                @Override
//                public Object map(Tuple4<String, Integer, Long, Long> sessionSummary)
//                        throws Exception {
//
//                    SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss");
//
//                    String minTime
//                            = format.format(new Date(Long.valueOf(sessionSummary.f2)));
//
//                    String maxTime
//                            = format.format(new Date(Long.valueOf(sessionSummary.f3)));
//
//                    System.out.println("--- Session Summary: " + (new Date()).toString()
//                            + " - User: " + sessionSummary.f0
//                            + " - Start Time: " + minTime
//                            + " - End Time: " + maxTime
//                            + " - Count: " + sessionSummary.f1);
//
//                    return null;
//                }
//            });

            /****************************************************************************
             *            Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the Kafka Stream generator on a separate thread to generate Kafka Stream as the job runs
            Utils.printHeader("Starting Kafka Stream Generator...");
            Thread kafkaGeneratorThread = new Thread(new KafkaStreamDataGenerator());
            kafkaGeneratorThread.start();

            /* Flink does lazy execution (it does not execute any code until an execute method or a data write operation is called).
            Here we explicitly call execute() on the streamEnv object to trigger program execution. */
            streamEnv.execute("Flink Streaming - Windowing Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
