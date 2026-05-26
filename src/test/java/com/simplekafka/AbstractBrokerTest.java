package com.simplekafka;

import com.simplekafka.broker.SimpleKafkaBroker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Base class for all integration tests.
 * Starts a broker on a dynamically chosen port in a background thread,
 * waits until it is ready, then tears everything down afterward.
 */
abstract class AbstractBrokerTest {

    private static final String LOG_DIR = "/tmp/kraft-combined-logs";

    protected SimpleKafkaBroker broker;
    protected int brokerPort;
    protected String brokerHost = "localhost";
    private Thread brokerThread;

    @BeforeEach
    void startBroker() throws Exception {
        cleanLogDir();
        brokerPort = findFreePort();
        broker = new SimpleKafkaBroker(brokerPort, 1);

        brokerThread = new Thread(() -> broker.start(), "broker-thread");
        brokerThread.setDaemon(true);
        brokerThread.start();

        // Wait until the port is listening (up to 10 s).
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(brokerHost, brokerPort), 100);
                break;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
    }

    @AfterEach
    void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
        }
        if (brokerThread != null) {
            brokerThread.join(5000);
        }
        cleanLogDir();
    }

    // -------------------------------------------------------- helpers

    private int findFreePort() throws IOException {
        try (java.net.ServerSocket ss = new java.net.ServerSocket(0)) {
            return ss.getLocalPort();
        }
    }

    protected void createTopic(String name, int partitions) throws IOException {
        broker.createTopic(name, partitions, (short) 1);
    }

    /**
     * Opens a Socket to the test broker and sends/receives a framed request.
     * Returns the response body (after response header).
     */
    protected ByteBuffer sendRequest(ByteBuffer framedRequest) throws IOException {
        return KafkaWireHelpers.sendRawSocket(brokerHost, brokerPort, framedRequest);
    }

    /**
     * Connects to the broker and returns the raw Socket.
     */
    protected Socket connect() throws IOException {
        return KafkaWireHelpers.connectSocket(brokerHost, brokerPort);
    }

    private final AtomicInteger correlationId = new AtomicInteger(100);
    protected int nextCorrelationId() { return correlationId.incrementAndGet(); }

    void cleanLogDir() throws IOException {
        Path dir = Path.of(LOG_DIR);
        if (Files.exists(dir)) {
            try (Stream<Path> walk = Files.walk(dir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(f -> {
                            try { Files.deleteIfExists(f.toPath()); } catch (IOException ignored) {}
                        });
            } catch (Exception ignored) {}
        }
    }
}
