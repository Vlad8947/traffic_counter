package ru.goncharov.traffic_counter;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.pcap4j.core.PcapNativeException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static ru.goncharov.traffic_counter.Constant.*;

public class Main {

    private static String master = "local[1]"; // Master setting for spark config
    private static String trafficNetAddress; // IP-address for traffic counting

    private static Connection connection; // sql-connection
    private static JavaStreamingContext jsc; // for streaming from kafka topic

    private static Producer producer; // Producer for send messages on kafka topic
    private static Limit limit; // Byte limits for count
    private static TrafficListener trafficListener; // Counts traffic

    public static void main(String[] args) throws Exception {
        // Read arguments if exist
        readArgs(args);

        // Initialisation of Limit, JavaStreamingContext, Producer, and shut down logic.
        init();

        // Start jsc and traffic listener
        start();

        // Close all processes
        close();
    }

    // Start main logic: consumer stream, traffic listener
    private static void start() throws PcapNativeException, InterruptedException {
        // Start consumer stream from kafka topic
        jsc.start();

        // TrafficListener initialisation and start thread
        trafficListener = new TrafficListener(producer, limit, trafficNetAddress);
        Thread listenerThread = new Thread(trafficListener);
        listenerThread.setDaemon(true);
        listenerThread.start();

        // Await while stream is working
        jsc.awaitTermination();
    }

    // Read received arguments
    private static void readArgs(String[] args) {
        // If args array is empty, return
        if (args.length == 0) {
            return;
        }
        String arg;
        // Process all arguments
        for(int i = 0; i < args.length; i++) {
            // Transform argument to lower case
            arg = args[i].toLowerCase();
            // Find parameter
            switch (arg) {
                case "--ip":
                    // If value for ip is exist
                    if (i + 1 < args.length) {
                        trafficNetAddress =  args[++i];
                    } else {
                        printErrorAndExit("IP value was not entered");
                    }
                    break;

                // If parameter name was not found
                default:
                    printErrorAndExit("Nonexistent parameter " + arg);
            }
        }
    }

    // Print error message and exit program
    public static void printErrorAndExit(String message) {
        System.err.println(message);
        close();
        System.exit(1);
    }

    // Initialisation for all main parameters
    private static void init() {
        sqlAndLimitInit();
        javaStreamingContextInit();
        directKafkaStreamInit();
        producerInit();
        shutDownInit();
    }

    // Logic of shut down command (Ctrl+C)
    private static void shutDownInit() {
        Thread shutDownThread = new Thread(Main::close);
        Runtime.getRuntime().addShutdownHook(shutDownThread);
    }


    // Initialisation of kafka topic producer
    private static void producerInit() {
        producer = new Producer();
    }

    // Close all processes in program
    private static void close() {
        jsc.close();
        try {
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            trafficListener.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            limit.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            // Time for correct termination of processes
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // Initialisation of sql-connection and limit-object
    private static void sqlAndLimitInit() {
        sqlConnectionInit();
        limit = new Limit(connection);
    }

    // Initialisation of sql-connection
    private static void sqlConnectionInit() {
        try {
            connection = DriverManager.getConnection(DB_URL, DB_USER_NAME, DB_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    // Initialisation of consumer stream
    private static void javaStreamingContextInit() {
        SparkConf sparkConf = new SparkConf().setMaster(master).setAppName(APP_Name);
        jsc = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        jsc.sparkContext().setLogLevel("WARN");
    }

    // Kafka stream initialisation
    private static void directKafkaStreamInit() {
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", CONSUMER_ADDRESS);
        Set<String> topics = Collections.singleton(ALERT_TOPIC);

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jsc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
            rdd.foreach(record -> System.out.println(record._2));
        });
    }

}
