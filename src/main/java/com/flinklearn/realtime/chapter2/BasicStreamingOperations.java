package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.File;

/*
A Flink program that reads a file stream, computes a Map-Reduce operation, and writes the result to a file sink.
 */

public class BasicStreamingOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                         Set up Flink environment
             ****************************************************************************/

            //Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            /* Set the parallelism to 1 for now to ensure there's only one Flink task slot processing all events in the job,
            so that the event flow is processed in sequence; otherwise, multiple threads can change the order of event processing. */
            streamEnv.setParallelism(1);

            /****************************************************************************
             *                   Read CSV File Stream into a DataStream
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
                        //The map function will continuously process new records as they arrive into the DataStream
                        .map( (MapFunction<String, AuditTrail>) auditStr -> {
                            System.out.println("--- Received Record : " + auditStr); //Print the received record to check its output
                            return new AuditTrail(auditStr); //Interpret the string and convert it to an AuditTrail object
                        } );

            /****************************************************************************
             *                           Perform computations
            ****************************************************************************/

            //Print the count of records received by the DataStream every 3 secs
            MapCountPrinter.printCount(
                    auditTrailObj.map(i -> (Object)i),
                    "Audit Trail count : Last 3 secs"
            );

            //Window by 3 secs, count the number of records and save to output
            DataStream<Tuple2<String,Integer>> recordCount
                    = auditTrailObj
                        .map(i -> new Tuple2<String,Integer>
                                (String.valueOf(System.currentTimeMillis()),1))  //Each record is counted as 1
                        .returns(Types.TUPLE(Types.STRING,Types.INT))
                        .timeWindowAll(Time.seconds(3))  //Use a time window of 3 secs
                        //Use Reduce to accumulate the count of records in the interval, while keeping the starting timestamp
                        .reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1));

            /****************************************************************************
             *                     Write the summary to a file sink
             ****************************************************************************/

            /* Flink allows output of streaming operations to be sent to various types of streaming sinks, including
            Kafka, Kinesis and Cassandra. In this case, we will write the output to a file system sink.*/

            //Define output directory to store summary information, and clean out existing files in the directory
            String outputDir = "data/five_sec_summary";
            FileUtils.cleanDirectory(new File(outputDir));

            //Set up a streaming file sink pointed to the output directory
            final StreamingFileSink<Tuple2<String,Integer>> countSink
                    = StreamingFileSink
                        .forRowFormat(
                                new Path(outputDir),
                                // Declare data type to match the data that is being written
                                new SimpleStringEncoder<Tuple2<String,Integer>>("UTF-8")
                        )
                        .build();

            //Attach the sink to the DataStream
            recordCount.addSink(countSink);

            /****************************************************************************
             *           Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the File Stream generator on a separate thread to generate file stream as the job runs
            Utils.printHeader("Starting File Stream Generator...");
            Thread fileStreamThread = new Thread(new FileStreamDataGenerator());
            fileStreamThread.start();

            /* Flink does lazy execution (it does not execute any code until an execute method or a data write operation is called).
            Here we explicitly call execute() on the streamEnv object to trigger program execution. */
            streamEnv.execute("Flink Streaming - Basic Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
