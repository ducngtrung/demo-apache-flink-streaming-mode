package com.flinklearn.realtime.chapter4;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Date;
import java.util.Properties;

/*
A Flink program that processes a watermarked data stream based on Event Time and writes the result to a Kafka sink.
 */

public class EventTimeOperations {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                         Set up Flink environment
             ****************************************************************************/

            //Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
            /* Set the parallelism to 1 for now to ensure there's only one Flink task slot processing all events in the job,
            so that the event flow is processed in sequence; otherwise, multiple threads can change the order of event processing. */
            streamEnv.setParallelism(1);

            /****************************************************************************
             *                  Read CSV File Stream into a DataStream
             ****************************************************************************/

            //Define directory to monitor for new files
            String dataDir = "data/raw_audit_trail";

            /* In order to read data from a data source, Flink needs to create a data source connection and attach it to a DataStream object.
            For a file-based data source, we define a text input format pointed to the directory where the files are created. */
            TextInputFormat auditFormat = new TextInputFormat(
                    new Path(dataDir)
            );

            //Create a DataStream to read content from the directory
            DataStream<String> auditTrailStr
                    = streamEnv.readFile(
                        auditFormat,    //Specify the file format
                        dataDir,        //Specify the directory to monitor
                        FileProcessingMode.PROCESS_CONTINUOUSLY,  //Flink will continuously monitor the directory for any new or recently changed files
                        1000    //Monitor interval (1 sec)
                    );

            //Use Map to convert each record in the DataStream to an AuditTrail object (POJO class)
            DataStream<AuditTrail> auditTrailObj
                    = auditTrailStr
                        .map( (MapFunction<String, AuditTrail>) auditStr -> {
                            System.out.println("--- Received File Record : " + auditStr);
                            return new AuditTrail(auditStr);
                        } );

            /****************************************************************************
             *                     Set up Event Time and Watermarks
             ****************************************************************************/

            //Create a Data Stream with watermarks
            DataStream<AuditTrail> auditTrailWithET
                    =auditTrailObj.assignTimestampsAndWatermarks(
                            (new AssignerWithPunctuatedWatermarks<AuditTrail>() {

                            //Extract Event timestamp value.
                            @Override
                            public long extractTimestamp(
                                    AuditTrail auditTrail,
                                    long previousTimeStamp) {

                                return auditTrail.getTimestamp();
                                }

                            //Extract Watermark
                            transient long currWaterMark = 0L;
                            int delay = 10000;
                            int buffer = 2000;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(
                                    AuditTrail auditTrail,
                                    long newTimestamp) {

                                long currentTime = System.currentTimeMillis();
                                if (currWaterMark == 0L) {
                                    currWaterMark = currentTime;
                                }
                                //update watermark every 10 seconds
                                else if ( currentTime - currWaterMark > delay) {
                                    currWaterMark = currentTime;
                                }
                                //return watermark adjusted to bufer
                                return new Watermark(
                                        currWaterMark - buffer);

                            }

                        }));

            /****************************************************************************
             *                     Process a watermarked DataStream
             ***************************************************************************/

            //Create a Separate Trail for Late events
            final OutputTag<Tuple2<String,Integer>> lateAuditTrail
                    = new OutputTag<Tuple2<String,Integer>>("late-audit-trail"){};

            SingleOutputStreamOperator<Tuple2<String, Integer>> finalTrail

                    = auditTrailWithET

                        .map(i -> new Tuple2<String, Integer> //get event timestamp and count
                                (String.valueOf(i.getTimestamp()), 1))
                        .returns(Types.TUPLE(Types.STRING, Types.INT))

                        .timeWindowAll(Time.seconds(1)) //Window by 1 second

                        .sideOutputLateData(lateAuditTrail) //Handle late data

                        .reduce((x, y) -> //Find total records every second
                                (new Tuple2<String, Integer>(x.f0, x.f1 + y.f1)))

                        //Pretty print
                        .map(new MapFunction<Tuple2<String, Integer>,
                                Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer>
                            map(Tuple2<String, Integer> minuteSummary)
                                    throws Exception {

                                String currentTime = (new Date()).toString();
                                String eventTime
                                        = (new Date(Long.valueOf(minuteSummary.f0))).toString();

                                System.out.println(ANSI_GREEN + "Summary : "
                                        + " Current Time : " + currentTime
                                        + " Event Time : " + eventTime
                                        + " Count :" + minuteSummary.f1 + ANSI_RESET);

                                return minuteSummary;
                            }
                        });


            //Collect late events and process them later.
            DataStream<Tuple2<String, Integer>> lateTrail
                    =  finalTrail.getSideOutput(lateAuditTrail);


            /****************************************************************************
             *                Send the processed results to a Kafka sink
             ****************************************************************************/

            //Setup Properties for Kafka connection
            Properties properties = new Properties();
            properties.setProperty("bootstrap.servers", "localhost:9092");

            //Create a Producer for Kafka
            FlinkKafkaProducer<String> kafkaProducer
                    = new FlinkKafkaProducer<String>(
                            //Topic Name
                            "flink.kafka.streaming.sink",

                            //Serialization for String data.
                            (new KafkaSerializationSchema<String>() {

                                @Override
                                public ProducerRecord<byte[], byte[]>
                                    serialize(String s, @Nullable Long aLong) {

                                    return (new ProducerRecord<byte[],byte[] >
                                            ("flink.kafka.streaming.sink",
                                                    s.getBytes()));
                                }
                            }),

                            properties,
                            FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

            //Publish to Kafka
            finalTrail //Convert to String and write to Kafka
                    .map(new MapFunction<Tuple2<String, Integer>, String>() {

                        @Override
                        public String map(Tuple2<String, Integer> finalTrail)
                                throws Exception {

                            return finalTrail.f0 + " = " + finalTrail.f1;
                        }
                    })
                    //Add Producer to Sink
                    .addSink(kafkaProducer);

            /****************************************************************************
             *           Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the File Stream generator on a separate thread to generate file stream as the job runs
            Utils.printHeader("Starting File Stream Generator...");
            Thread fileStreamThread = new Thread(new FileStreamDataGenerator());
            fileStreamThread.start();

            /* Flink does lazy execution (it does not execute any code until an execute method or a data write operation is called).
            Here we explicitly call execute() on the streamEnv object to trigger program execution. */
            streamEnv.execute("Flink Streaming - Event Time Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
