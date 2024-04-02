package com.flinklearn.realtime.datasource;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

/*************************************************************************************
 * This Generator use Kafka Producer to generate a stream of events
 * To run this example, you need a running instance of Kafka with 2 topics: flink.kafka.streaming.source and flink.kafka.streaming.sink
 * This can be used for streaming data consumption by Flink
 *************************************************************************************/

public class KafkaStreamDataGenerator implements Runnable {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_PURPLE = "\u001B[35m";

    public static void main(String[] args) {
        KafkaStreamDataGenerator kafkaGenerator = new KafkaStreamDataGenerator();
        kafkaGenerator.run();
    }

    public void run() {

        try {

            //Set connection properties to Kafka cluster
            Properties kafkaProps = new Properties();
            kafkaProps.put("client.id", "TestKafkaProducer");
            kafkaProps.put("key.serializer", StringSerializer.class.getName());
            kafkaProps.put("value.serializer", StringSerializer.class.getName());

            //Specify brokers list: "host:port,another_host:port,..."
            kafkaProps.put("bootstrap.servers", "10.82.81.128:9092,10.82.81.157:9092");

            //Set SASL authentication properties
            kafkaProps.put("security.protocol", "SASL_PLAINTEXT");
            kafkaProps.put("sasl.mechanism", "PLAIN");
            kafkaProps.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='ani' password='YW5pY2x1c3Rlcg';");

            //Optional properties
            kafkaProps.put("connections.max.idle.ms", "10000");
            kafkaProps.put("metadata.max.age.ms", "5000");
            kafkaProps.put("retries", "3");
            kafkaProps.put("retry.backoff.ms", "5000");
            kafkaProps.put("request.timeout.ms", "10000");

            //Create a Kafka producer based on the properties
            Producer<String,String> kafkaProducer = new KafkaProducer<>(kafkaProps);

            //Define list of application users
            List<String> appUser = new ArrayList<String>();
            appUser.add("Tom");
            appUser.add("Harry");
            appUser.add("Bob");

            //Define list of application operations
            List<String> appOperation = new ArrayList<String>();
            appOperation.add("Create");
            appOperation.add("Modify");
            appOperation.add("Query");
            appOperation.add("Delete");

            //Define list of application entities
            List<String> appEntity = new ArrayList<String>();
            appEntity.add("Customer");
            appEntity.add("SalesRep");

            //Define a random number generator
            Random random = new Random();

            /* Generate 20 sample audit records, each record produces an event
            (using the same structure as CSV files in the data/raw_audit_trail folder) */
            for (int i=0; i<20; i++) {

                //Capture current timestamp
                String currentTime = String.valueOf(System.currentTimeMillis());
                //Pick a random user
                String user = appUser.get(random.nextInt(appUser.size()));
                //Pick a random operation
                String operation = appOperation.get(random.nextInt(appOperation.size()));
                //Pick a random entity
                String entity= appEntity.get(random.nextInt(appEntity.size()));
                //Pick a random duration (from 1 to 10) for the operation
                String duration = String.valueOf(random.nextInt(10) + 1);
                //Pick a random value (from 1 to 4) for the number of changes
                String changeCount = String.valueOf(random.nextInt(4) + 1);

                //Create a text array to hold this record
                String[] textArr = {String.valueOf(i), user, entity, operation, currentTime, duration, changeCount};

                //Create a Kafka producer record
                ProducerRecord<String,String> record =
                        new ProducerRecord<String,String>(
                                "flink.kafka.streaming.source", //the Kafka topic to which you publish the record
                                currentTime,    //key
                                String.join(",", textArr)   //value
                        );
                //Publish the producer record to Kafka and collect its metadata for future use
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();

                //Use ANSI code to print colored text in console
                System.out.println(ANSI_PURPLE + "Kafka Stream Generator : Published Event : "
                            + String.join(",", textArr) + ANSI_RESET);

                //Sleep for a random time (up to 1 sec) before creating next record
                Thread.sleep(random.nextInt(1000) + 1);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
