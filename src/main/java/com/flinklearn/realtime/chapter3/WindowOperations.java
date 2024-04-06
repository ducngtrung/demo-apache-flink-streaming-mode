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
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/*
A Flink program that reads a Kafka topic stream and perform window operations on Sliding window and Session window.
 */

public class WindowOperations {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                         Set up Flink environment
             ****************************************************************************/

            //Set up the streaming execution environment
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
            kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='ani' password='YW5pY2x1c3Rlcg';");

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
                                Time.seconds(10),   //Window size
                                Time.seconds(5))    //Slide by 5
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
                            SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
                            String minTime = timeFormat.format(new Date(slidingSummary.f2));
                            String maxTime = timeFormat.format(new Date(slidingSummary.f3));
                            System.out.println(ANSI_GREEN + "--- Sliding Summary: " + (new Date()).toString()
                                + " - Min Time: " + minTime
                                + " - Max Time: " + maxTime
                                + " - Count: " + slidingSummary.f1 + ANSI_RESET);
                            return null;
                        }
                    } );

            /****************************************************************************
             *                            Use Session window
             ****************************************************************************/

            /* Compute the count of events, minimum timestamp and maximum timestamp for each session window, partitioned by user and use a window gap of 5 seconds.
            This means if a given user has not produced any new event in the last 5 seconds, the current session ends and the next session will start. */
            DataStream<Tuple4<String, Integer, Long, Long>> sessionSummary
                    = auditTrailObj
                        .map(i -> new Tuple4<String, Integer, Long, Long>
                                (i.getUser(), //User
                                1,            //Count each record as 1
                                i.getTimestamp(),   //Minimum Timestamp
                                i.getTimestamp()))  //Maximum Timestamp
                        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LONG, Types.LONG))
                        .keyBy(0)   //Partition by user
                        .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))) //"Processing Time" means that Flink will use timestamps on íts processor as timestamps for windowing. Processing Time is different from Event Time, which is the time when an event actually happened.
                        .reduce((x, y) -> new Tuple4<String, Integer, Long, Long>
                                        (x.f0,                  //User
                                        x.f1 + y.f1,            //Accumulate the count of records
                                        Math.min(x.f2, y.f2),   //Compare to find the minimum timestamp
                                        Math.max(x.f3, y.f3))); //Compare to find the maximum timestamp

            //Pretty print
            sessionSummary
                    .map( new MapFunction<Tuple4<String, Integer, Long, Long>, Object>() {
                        @Override
                        public Object map(Tuple4<String, Integer, Long, Long> sessionSummary) throws Exception {
                            SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
                            String minTime = timeFormat.format(new Date(sessionSummary.f2));
                            String maxTime = timeFormat.format(new Date(sessionSummary.f3));
                            System.out.println(ANSI_BLUE + "--- Session Summary: " + (new Date()).toString()
                                    + " - User: " + sessionSummary.f0
                                    + " - Min Time: " + minTime
                                    + " - Max Time: " + maxTime
                                    + " - Count: " + sessionSummary.f1 + ANSI_RESET);
                            return null;
                        }
                    } );

            /****************************************************************************
             *            Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the Kafka Stream generator on a separate thread to generate Kafka stream as the job runs
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
