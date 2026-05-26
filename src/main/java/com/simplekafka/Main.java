package com.simplekafka;

import com.simplekafka.broker.SimpleKafkaBroker;

/**
 * Entry point for the SimpleKafka broker.
 * <p>
 * Usage: java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.Main [port] [brokerId]
 * <p>
 * Default: port=9092, brokerId=1
 */
public class Main {

    public static void main(String[] args) {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 9092;
        int brokerId = args.length > 1 ? Integer.parseInt(args[1]) : 1;

        SimpleKafkaBroker broker = new SimpleKafkaBroker(port, brokerId);
        broker.start();
    }
}
