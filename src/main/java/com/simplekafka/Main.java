package com.simplekafka;

import com.simplekafka.broker.SimpleKafkaBroker;
import com.simplekafka.shared.Config;

import java.io.File;

/**
 * Entry point for the SimpleKafka broker.
 * <p>
 * Usage:
 *   java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.Main [port] [brokerId]
 *   java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.Main --config server.properties
 * <p>
 * Default: port=9092, brokerId=1
 */
public class Main {

    public static void main(String[] args) throws Exception {
        SimpleKafkaBroker broker;

        if (args.length > 0 && "--config".equals(args[0])) {
            if (args.length < 2) {
                System.err.println("Usage: Main --config <properties-file>");
                System.exit(1);
            }
            Config config = Config.fromProperties(new File(args[1]));
            broker = new SimpleKafkaBroker(config);
        } else {
            int port = args.length > 0 ? Integer.parseInt(args[0]) : 9092;
            int brokerId = args.length > 1 ? Integer.parseInt(args[1]) : 1;
            broker = new SimpleKafkaBroker(port, brokerId);
        }

        broker.start();
    }
}
