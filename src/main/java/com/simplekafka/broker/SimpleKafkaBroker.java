package com.simplekafka.broker;

import com.simplekafka.broker.handlers.ApiVersionsHandler;
import com.simplekafka.broker.handlers.DescribeTopicPartitionsHandler;
import com.simplekafka.broker.handlers.FetchHandler;
import com.simplekafka.broker.handlers.ProduceHandler;
import com.simplekafka.metadata.ClusterMetadataLog;
import com.simplekafka.metadata.ClusterMetadataStore;
import com.simplekafka.metadata.PartitionRecord;
import com.simplekafka.metadata.TopicRecord;
import com.simplekafka.shared.RequestHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main broker server that accepts TCP connections on a configurable port.
 * Uses blocking I/O with a thread-per-connection model via virtual threads.
 */
public class SimpleKafkaBroker {

    private static final Logger LOGGER = Logger.getLogger(SimpleKafkaBroker.class.getName());

    private final int port;
    private final int brokerId;
    private volatile boolean running;
    private volatile boolean isController = false;
    private int controllerEpoch = 0;
    private ServerSocket serverSocket;

    private final BrokerInfo brokerInfo;
    private final ApiVersionsHandler apiVersionsHandler = new ApiVersionsHandler();
    private final ClusterMetadataStore metadataStore = new ClusterMetadataStore();
    private final DescribeTopicPartitionsHandler describeTopicPartitionsHandler =
            new DescribeTopicPartitionsHandler(metadataStore);
    private final ProduceHandler produceHandler = new ProduceHandler(metadataStore);
    private final FetchHandler fetchHandler = new FetchHandler(metadataStore, produceHandler);
    private final ClusterMetadataLog metadataLog = new ClusterMetadataLog();

    public SimpleKafkaBroker(int port) {
        this(port, 1);
    }

    public SimpleKafkaBroker(int port, int brokerId) {
        this.port = port;
        this.brokerId = brokerId;
        this.running = false;
        this.brokerInfo = new BrokerInfo(brokerId, "localhost", port);
    }

    public ClusterMetadataStore getMetadataStore() {
        return metadataStore;
    }

    public BrokerInfo getBrokerInfo() {
        return brokerInfo;
    }

    public boolean isController() {
        return isController;
    }

    /**
     * Starts the broker: loads metadata, elects controller, opens server socket,
     * and enters the accept loop.
     */
    public void start() {
        try {
            loadMetadata();
            electController();

            serverSocket = new ServerSocket();
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(port));
            running = true;

            LOGGER.info("SimpleKafka broker " + brokerId + " started on port " + port +
                    " (controller=" + isController + ")");

            acceptLoop();

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Broker failed to start", e);
        } finally {
            metadataLog.close();
        }
    }

    private void loadMetadata() {
        try {
            metadataLog.open();
            metadataLog.loadInto(metadataStore);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Could not load metadata log, starting fresh", e);
        }
    }

    private void electController() {
        isController = true;
        controllerEpoch++;
        LOGGER.info("Broker " + brokerId + " elected as controller (epoch=" + controllerEpoch + ")");
    }

    public void createTopic(String topicName, int partitionCount, short replicationFactor) throws IOException {
        UUID topicId = UUID.randomUUID();
        TopicRecord topicRecord = new TopicRecord(topicName, topicId);

        metadataLog.writeTopicRecord(topicRecord);
        metadataStore.addTopic(topicRecord);

        for (int i = 0; i < partitionCount; i++) {
            PartitionRecord partitionRecord = new PartitionRecord(
                    i, topicId, brokerId, 0,
                    List.of(brokerId), List.of(brokerId));

            metadataLog.writePartitionRecord(partitionRecord);
            metadataStore.addPartition(partitionRecord);
        }

        LOGGER.info("Created topic '" + topicName + "' with " + partitionCount +
                " partitions (topicId=" + topicId + ")");
    }

    private void acceptLoop() {
        while (running) {
            try {
                serverSocket.setSoTimeout(200);
                Socket clientSocket;
                try {
                    clientSocket = serverSocket.accept();
                } catch (java.net.SocketTimeoutException e) {
                    continue;
                }

                LOGGER.info("Accepted connection from " + clientSocket.getRemoteSocketAddress());

                Thread.ofVirtual().start(() -> handleClient(clientSocket));

            } catch (IOException e) {
                if (running) {
                    LOGGER.log(Level.WARNING, "Error accepting connection", e);
                }
            }
        }
    }

    private void handleClient(Socket socket) {
        try {
            socket.setTcpNoDelay(true);
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            while (running && !socket.isClosed()) {
                // Read message_size (4 bytes)
                byte[] sizeBytes = readN(in, 4);
                if (sizeBytes == null) break;

                int messageSize = ByteBuffer.wrap(sizeBytes).getInt();

                if (messageSize <= 0) {
                    LOGGER.warning("Invalid message size: " + messageSize);
                    break;
                }

                // Read the message payload
                byte[] payload = readN(in, messageSize);
                if (payload == null) break;

                ByteBuffer requestBuf = ByteBuffer.wrap(payload);

                try {
                    RequestHeader header = RequestHeader.parse(requestBuf);
                    ByteBuffer response = dispatchRequest(header, requestBuf);

                    if (response != null) {
                        byte[] respBytes = new byte[response.remaining()];
                        response.get(respBytes);
                        out.write(respBytes);
                        out.flush();
                    }
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Failed to process request", e);
                    break;
                }
            }
        } catch (IOException e) {
            // Client disconnected or error
        } finally {
            try { socket.close(); } catch (IOException ignored) {}
        }
    }

    private byte[] readN(InputStream in, int n) throws IOException {
        byte[] buf = new byte[n];
        int offset = 0;
        while (offset < n) {
            int read = in.read(buf, offset, n - offset);
            if (read == -1) return null;
            offset += read;
        }
        return buf;
    }

    private ByteBuffer dispatchRequest(RequestHeader header, ByteBuffer requestBody) {
        switch (header.getApiKey()) {
            case 18: return apiVersionsHandler.handle(header);
            case 75: return describeTopicPartitionsHandler.handle(header, requestBody);
            case 0: return produceHandler.handle(header, requestBody);
            case 1: return fetchHandler.handle(header, requestBody);
            default:
                LOGGER.warning("Unsupported API key: " + header.getApiKey());
                return null;
        }
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null && !serverSocket.isClosed()) {
                serverSocket.close();
            }
        } catch (IOException ignored) {}
        metadataLog.close();
        LOGGER.info("Broker shutting down");
    }
}
