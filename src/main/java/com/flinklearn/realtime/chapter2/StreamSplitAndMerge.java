package com.flinklearn.realtime.chapter2;

import com.flinklearn.realtime.common.MapCountPrinter;
import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.datasource.FileStreamDataGenerator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/*
A Flink program to demonstrate splitting data streams (using Side Output) and merging multiple streams.
 */
public class StreamSplitAndMerge {

    public static void main(String[] args) {

        try{

            /****************************************************************************
             *                          Set up Flink environment
             ****************************************************************************/

            // Set up the streaming execution environment
            final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

            //Keeps the ordering of records. Else multiple threads can change the sequence of printing.

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

            /***************************************************************************
             *            Split the stream into two streams based on Entity
             ***************************************************************************/

            /* There are 2 entity values: Customer and SalesRep.
            We will use Side Output to obtain separate DataStreams for these entities. */

            //Define a unique output tag for the Side Output
            final OutputTag<Tuple2<String,Integer>> salesRepTag
                    = new OutputTag<Tuple2<String,Integer>>("sales-rep"){};

            //Emit main output with AuditTrail object for Customer records, while also collecting side output for SalesRep records
            SingleOutputStreamOperator<AuditTrail> customerTrail
                    = auditTrailStr
                        /* This process function takes a String input and outputs an AuditTrail object.
                        It implements the processElement method that will be called for each record in the DataStream. */
                        .process( new ProcessFunction<String,AuditTrail>() {
                            @Override
                            public void processElement(
                                String auditStr,    //Input
                                Context runtimeContext,    //The current context
                                Collector<AuditTrail> auditTrailCollector   //Collector to collect output AuditTrail objects
                            ) {
                                System.out.println("--- Received Record : " + auditStr);

                                //Interpret the string and convert it to an AuditTrail object
                                AuditTrail auditTrail = new AuditTrail(auditStr);

                                //Create an output Tuple with username and a count of 1
                                Tuple2<String,Integer> userCount = new Tuple2<String,Integer>(auditTrail.user,1);

                                if (auditTrail.getEntity().equals("Customer")) {
                                    //Add the AuditTrail object to the main output collector
                                    auditTrailCollector.collect(auditTrail);
                                } else {
                                    //Use context to set side output by passing the side output tag and the data object
                                    runtimeContext.output(salesRepTag, userCount);
                                }
                            }
                        } );

            //Create a separate DataStream for all records that have been tagged as side output
            DataStream<Tuple2<String,Integer>> salesRepTrail = customerTrail.getSideOutput(salesRepTag);

            //Print the count of Customer records received by the DataStream every 3 secs
            MapCountPrinter.printCount(
                    customerTrail.map(i -> (Object)i),
                    "Customer records in Trail : Last 3 secs");

            //Print the count of SalesRep records received by the DataStream every 3 secs
            MapCountPrinter.printCount(
                    salesRepTrail.map(i -> (Object)i),
                    "SalesRep records in Trail : Last 3 secs");

            /****************************************************************************
             *                       Merge two streams into one
             ****************************************************************************/

            /* The customerTrail and salesRepTrail streams are of different formats. One is a POJO object called and the other is a Tuple. We can vertically combine them and push them into the same format using the connect() operator. */
            ConnectedStreams<AuditTrail, Tuple2<String,Integer>> mergedTrail
                    = customerTrail
                        .connect(salesRepTrail);

            /* The connected stream (mergedTrail) has both data types (POJO object and Tuple), but it is still not fully merged. We need to execute the CoMapFunction to complete the merge. */
            DataStream<Tuple3<String,String,Integer>> processedTrail
                = mergedTrail
                    .map( new CoMapFunction<
                                    AuditTrail, //Stream 1
                                    Tuple2<String,Integer>, //Stream 2
                                    Tuple3<String,String,Integer> //Output
                             >() {

                        //Process stream 1 (containing Customer records)
                        @Override
                        public Tuple3<String,String,Integer>
                            map1(AuditTrail customerTrail) throws Exception {
                                return new Tuple3<String,String,Integer>
                                        ("Stream-Customer",customerTrail.user,1);
                        }

                        //Process stream 2 (containing SalesRep records)
                        @Override
                        public Tuple3<String,String,Integer>
                            map2(Tuple2<String,Integer> salesRepTrail) throws Exception {
                                return new Tuple3<String,String,Integer>
                                        ("Stream-SalesRep",salesRepTrail.f0,1);
                        }

                        /* Both map methods need to emit data in the same output format. In this case, both of them emit a tuple of 3 attributes: the name of the stream, the username and a count of 1 for each record. The output is vertically combined as a single DataStream called processedTrail, with each record belonging to either of the individual input streams. */
                    } );

            //Run the combined DataStream through a Map function to print each record in that stream
            processedTrail
                    .map( new MapFunction<
                            Tuple3<String,String,Integer>,  //Input
                            Tuple3<String,String,Integer>   //Output
                            >() {
                        @Override
                        public Tuple3<String,String,Integer>
                            map(Tuple3<String,String,Integer> user) {
                                System.out.println("--- Merged Record for User : " + user);
                                return null;
                        }
                    } );
            /* When you look at the merged records in the console, you will see the name of the specific stream from which each record originated. */

            /****************************************************************************
             *           Set up data source and execute the streaming pipeline
             ****************************************************************************/

            //Start the File Stream generator on a separate thread to generate File Stream as the job runs
            Utils.printHeader("Starting FileStream Generator...");
            Thread genFileStreamThread = new Thread(new FileStreamDataGenerator());
            genFileStreamThread.start();

            /* Flink does lazy execution (it does not execute any code until an execute method or a data write operation is called). Here we explicitly call execute() on the streamEnv object to trigger program execution. */
            streamEnv.execute("Flink Streaming Split-Merge Example");

        }
        catch(Exception e) {
            e.printStackTrace();
        }

    }

}
