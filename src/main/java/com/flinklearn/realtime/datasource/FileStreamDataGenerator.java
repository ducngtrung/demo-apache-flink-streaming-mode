package com.flinklearn.realtime.datasource;

import com.opencsv.CSVWriter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/*************************************************************************************
 * This Generator generates a series of data files in the data/raw_audit_trail folder
 * It simulates an audit trail data source
 * This can be used for streaming data consumption by Flink
 *************************************************************************************/

public class FileStreamDataGenerator implements Runnable {

    public static final String ANSI_RESET = "\u001B[0m";
    public static final String ANSI_CYAN = "\u001B[36m";

    public static void main(String[] args) {
        FileStreamDataGenerator fileGenerator = new FileStreamDataGenerator();
        fileGenerator.run();
    }

    public void run() {

        try {

            //Define a data directory to output files, and clean out existing files in the directory
            String dataDir = "data/raw_audit_trail";
            FileUtils.cleanDirectory(new File(dataDir));

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

            //Generate 30 sample audit records, each record produces a CSV file
            for (int i=0; i<30; i++) {

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

                //Open a new file for this record
                FileWriter auditFile = new FileWriter(dataDir + "/audit_trail_" + i + ".csv");
                CSVWriter auditCSV = new CSVWriter(auditFile);

                //Write the audit record and close the file
                auditCSV.writeNext(textArr);
                System.out.println(ANSI_CYAN + "File Stream Generator : Created File : "
                            + Arrays.toString(textArr) + ANSI_RESET); //Use ANSI code to print colored text in console
                auditCSV.flush();
                auditCSV.close();

                //Sleep for a random time (up to 1 sec) before creating next record
                Thread.sleep(random.nextInt(1000) + 1);

            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
