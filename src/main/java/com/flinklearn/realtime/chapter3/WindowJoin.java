package com.flinklearn.realtime.chapter3;

import com.flinklearn.realtime.chapter2.AuditTrail;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import com.flinklearn.realtime.datasource.KafkaStreamDataGenerator;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/*
A Flink program that performs SQL-like horizontal join on two data streams: a file stream
and a Kafka topic stream in the same window.
 */

public class WindowJoin {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_GREEN = "\u001B[32m";
    public static final String ANSI_BLUE = "\u001B[34m";

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                         Set up Flink environment
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            /****************************************************************************
             *               Read CSV File Stream into the first DataStream
             ****************************************************************************/

            //Define directory to monitor for new files
            String dataDir = "data/raw_audit_trail";

            /* In order to read data from a data source, Flink needs to create a data source connection and attach it to a DataStream object.
            For a file-based data source, we define a text input format pointed to the directory where the files are created. */
            TextInputFormat auditFormat = new TextInputFormat(
                    new Path(dataDir)
            );

            //Create a DataStream to read content from the directory
            DataStream<String> fileTrailStr
                    = streamEnv.readFile(
                        auditFormat,    //Specify the file format
                        dataDir,        //Specify the directory to monitor
                        FileProcessingMode.PROCESS_CONTINUOUSLY,  //Flink will continuously monitor the directory for any new or recently changed files
                        1000    //Monitor interval (1 sec)
                    );

            //Use Map to convert each record in the DataStream to an AuditTrail object (POJO class)
            DataStream<AuditTrail> fileTrailObj
                    = fileTrailStr
                        .map( (MapFunction<String, AuditTrail>) auditStr -> {
                            System.out.println(ANSI_GREEN + "--- Received File Record : " + auditStr + ANSI_RESET);
                            return new AuditTrail(auditStr);
                        } );

            /****************************************************************************
             *              Read Kafka Topic Stream into the second DataStream
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
            DataStream<String> kafkaTrailStr = streamEnv.addSource(kafkaConsumer);

            //Use Map to convert each record in the DataStream to an AuditTrail object (POJO class)
            DataStream<AuditTrail> kafkaTrailObj
                    = kafkaTrailStr
                        .map( (MapFunction<String, AuditTrail>) auditStr -> {
                            System.out.println(ANSI_BLUE + "--- Received Kafka Record : " + auditStr + ANSI_RESET);
                            return new AuditTrail(auditStr);
                        } );
            /* When the Kafka Generator has started sending events to Kafka, the operation code consumes data from Kafka in real time and prints only the latest messages. */

            /****************************************************************************
             *                  Join both streams based on the same window
             ****************************************************************************/

            /* We will join both streams based on User, with a window of 5 seconds.
            For each matching record, we will output the username and the count. */
            DataStream<Tuple2<String, Integer>> joinCounts =
                    fileTrailObj.join(kafkaTrailObj)    //Join two data streams

                        // Use "where" to indicate the field on the first stream for joining
                        .where(new KeySelector<AuditTrail, String>() {
                            //Select the User field from the file record
                            @Override
                            public String getKey(AuditTrail auditTrail) throws Exception {
                                return auditTrail.getUser();
                            }
                        })

                        //Use "equalTo" to indicate the field on the second stream for joining
                        .equalTo(new KeySelector<AuditTrail, String>() {
                            //Select the User field from the Kafka record
                            @Override
                            public String getKey(AuditTrail auditTrail) throws Exception {
                                return auditTrail.getUser();
                            }
                        })

                        //Create a Tumbling window of 5 seconds
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        /* How would this window join work?
                        Within a given window, if there are 2 records for a given user in the first stream and
                        3 records for the same user in the second stream, the joined stream will have 6 records (2x3).
                        It is a Cartesian product of the matching records. */

                        /* Apply a custom Join Function against each matched combination of records.
                        This Join Function has access to the corresponding source records on both input data streams. */
                        .apply( new JoinFunction<
                                AuditTrail, AuditTrail,     //2 records as inputs
                                Tuple2<String,Integer>      //output (a Tuple with the username and a count of 1)
                                >() {
                            @Override
                            public Tuple2<String, Integer> join(AuditTrail fileTrail, AuditTrail kafkaTrail) throws Exception {
                                return new Tuple2<String, Integer>(fileTrail.getUser(), 1);
                            }
                        } );

            //Print the counts
            joinCounts.print();

            /****************************************************************************
             *            Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the File Stream generator on a separate thread to generate file stream as the job runs
            Utils.printHeader("Starting File Stream Generator...");
            Thread fileGeneratorThread = new Thread(new FileStreamDataGenerator());
            fileGeneratorThread.start();

            //Start the Kafka Stream generator on a separate thread to generate Kafka stream as the job runs
            Utils.printHeader("Starting Kafka Stream Generator...");
            Thread kafkaGeneratorThread = new Thread(new KafkaStreamDataGenerator());
            kafkaGeneratorThread.start();

            /* Flink does lazy execution (it does not execute any code until an execute method or a data write operation is called).
            Here we explicitly call execute() on the streamEnv object to trigger program execution. */
            streamEnv.execute("Flink Streaming - Window Join Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
