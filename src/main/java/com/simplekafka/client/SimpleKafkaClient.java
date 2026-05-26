package com.simplekafka.client;

import com.simplekafka.shared.primitives.CompactString;
import com.simplekafka.shared.primitives.Int16;
import com.simplekafka.shared.primitives.Int32;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * NIO-based client for connecting to a SimpleKafka broker.
 * Provides low-level send/receive operations using the Kafka wire protocol.
 */
public class SimpleKafkaClient {

    private static final AtomicInteger CORRELATION_ID = new AtomicInteger(0);

    private final String host;
    private final int port;
    private SocketChannel channel;

    public SimpleKafkaClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Connects to the broker.
     */
    public void connect() throws IOException {
        channel = SocketChannel.open();
        channel.configureBlocking(true);
        channel.connect(new InetSocketAddress(host, port));
        channel.finishConnect();
    }

    /**
     * Sends a request to the broker and reads the response.
     *
     * @param apiKey      the API key
     * @param apiVersion  the API version
     * @param clientId    the client ID
     * @param bodyBuilder builds the request body into a ByteBuffer
     * @return the response body (after the response header has been consumed)
     */
    public ByteBuffer sendRequest(short apiKey, short apiVersion, String clientId, ByteBuffer bodyBuilder) throws IOException {
        int correlationId = CORRELATION_ID.incrementAndGet();

        // Build the header.
        int headerSize = Int16.size() + Int16.size() + Int32.size() +
                CompactString.size(clientId) + 1; // +1 for TAG_BUFFER
        int bodySize = bodyBuilder.remaining();
        int messageSize = headerSize + bodySize;

        ByteBuffer request = ByteBuffer.allocate(Int32.size() + messageSize);
        Int32.write(request, messageSize);

        // Header v2.
        Int16.write(request, apiKey);
        Int16.write(request, apiVersion);
        Int32.write(request, correlationId);
        CompactString.write(request, clientId);
        request.put((byte) 0); // TAG_BUFFER

        // Body.
        request.put(bodyBuilder);
        request.flip();

        channel.write(request);

        // Read response.
        return readResponse(correlationId);
    }

    /**
     * Reads a response from the broker.
     *
     * @param expectedCorrelationId the expected correlation ID
     * @return the response body (after header)
     */
    private ByteBuffer readResponse(int expectedCorrelationId) throws IOException {
        // Read message_size.
        ByteBuffer sizeBuf = ByteBuffer.allocate(Int32.size());
        readFully(channel, sizeBuf);
        sizeBuf.flip();
        int messageSize = Int32.read(sizeBuf);

        // Read the full message.
        ByteBuffer message = ByteBuffer.allocate(messageSize);
        readFully(channel, message);
        message.flip();

        // Parse response header v1: correlation_id + TAG_BUFFER.
        int correlationId = Int32.read(message);
        if (correlationId != expectedCorrelationId) {
            throw new IOException("Correlation ID mismatch: expected " + expectedCorrelationId + ", got " + correlationId);
        }
        skipTagBuffer(message);

        return message;
    }

    /**
     * Reads exactly the remaining bytes from the channel.
     */
    private void readFully(SocketChannel channel, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int read = channel.read(buf);
            if (read == -1) {
                throw new IOException("Connection closed while reading");
            }
        }
    }

    /**
     * Skips a TAG_BUFFER (tagged fields) section.
     */
    private void skipTagBuffer(ByteBuffer buf) {
        int numTags = CompactString.readUnsignedVarint(buf);
        for (int i = 0; i < numTags; i++) {
            CompactString.readUnsignedVarint(buf); // tagId
            int size = CompactString.readUnsignedVarint(buf);
            buf.position(buf.position() + size);
        }
    }

    /**
     * Disconnects from the broker.
     */
    public void disconnect() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
            }
        }
    }

    public boolean isConnected() {
        return channel != null && channel.isOpen();
    }
}
