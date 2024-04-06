package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/*
A Flink program to demonstrate working on keyed streams.
In partitioning, we use the keyBy operator to partition data by one or more attributes in the data stream.
Flink distributes events in the DataStream to different task slots based on the keys (same key value to the same slot).
Partitioning by key is ideal for aggregation operations that aggregate on a specific key. Each key is then aggregated
locally in the task slot - this helps to optimize performance.
 */

public class KeyedStreamOperations {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                         Set up Flink environment
             ****************************************************************************/

            //Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            //Print the number of task slots available (unless explicitly defined, this defaults to the number of cores available on your local machine)
            System.out.println("\nTotal Parallel Task Slots : " + streamEnv.getParallelism());

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

            /****************************************************************************
             *             Partition by User, find running summary by User
             ****************************************************************************/

            //Use Map to convert each record in the DataStream to a Tuple with user and running total duration
            DataStream<Tuple2<String, Integer>> userDuration
                    = auditTrailStr
                        .map( new MapFunction<String, Tuple2<String,Integer>>() {
                             @Override
                             public Tuple2<String,Integer> map(String auditStr) {
                                 System.out.println("--- Received Record : " + auditStr);
                                 //Interpret the string and convert it to an AuditTrail object
                                 AuditTrail auditTrailObject = new AuditTrail(auditStr);
                                 return new Tuple2<String,Integer>
                                         (auditTrailObject.user,auditTrailObject.duration);
                             }
                        } )
                        .keyBy(0)  //Partition by user, and distribute the partitions to separate task slots
                        //Use Reduce to accumulate the duration of each user
                        .reduce((x,y) -> new Tuple2<String, Integer>(x.f0, x.f1 + y.f1));

            //Print user and running total duration
            userDuration.print();
            /* When you run the program and look at the console, note that the first number in the output (followed by the '>' symbol)
            is the task slot ID, which tells you the specific task slot that is processing a record. Records with the same username
            get assigned to the same slot. The total duration keeps getting updated as a running total. */

            /****************************************************************************
             *           Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the File Stream generator on a separate thread to generate file stream as the job runs
            Utils.printHeader("Starting File Stream Generator...");
            Thread fileStreamThread = new Thread(new FileStreamDataGenerator());
            fileStreamThread.start();

            /* Flink does lazy execution (it does not execute any code until an execute method or a data write operation is called).
            Here we explicitly call execute() on the streamEnv object to trigger program execution. */
            streamEnv.execute("Flink Streaming - Keyed Stream Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
