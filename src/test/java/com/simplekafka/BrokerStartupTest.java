package com.simplekafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Epic 1: Broker Startup & Protocol Negotiation")
class BrokerStartupTest extends AbstractBrokerTest {

    /** Helper: sends a framed request and returns the response (after 4-byte size). */
    private ByteBuffer send(ByteBuffer req) throws IOException {
        return KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
    }

    @Test
    @Timeout(15)
    @DisplayName("1.1 Broker starts and accepts TCP connections on its port")
    void brokerAcceptsTcpConnection() throws IOException {
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            assertTrue(s.isConnected(), "Socket should be connected");
        }
    }

    @Test
    @Timeout(15)
    @DisplayName("1.2 ApiVersions response includes keys 0, 1, 18, 75 with correct ranges")
    void apiVersionsIncludesRequiredKeys() throws IOException {
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "test-client");
        ByteBuffer resp = send(req);

        KafkaWireHelpers.skipResponseHeader(resp);
        List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);

        assertKeyRange(entries, (short) 0, (short) 0, (short) 11);
        assertKeyRange(entries, (short) 1, (short) 0, (short) 16);
        assertKeyRange(entries, (short) 18, (short) 0, (short) 4);
        assertKeyRange(entries, (short) 75, (short) 0, (short) 0);
        assertEquals(4, entries.size(), "Should have exactly 4 API entries");
    }

    @Test
    @Timeout(15)
    @DisplayName("1.3 Malformed request: zero message_size triggers error/closure")
    void malformedZeroMessageSize() throws Exception {
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            OutputStream out = s.getOutputStream();
            ByteBuffer bad = ByteBuffer.allocate(4);
            bad.putInt(0);
            bad.flip();
            out.write(bad.array());
            out.flush();
            Thread.sleep(200);
        }
        // Verify broker is still alive
        try (Socket s2 = new Socket(brokerHost, brokerPort)) {
            int corrId = nextCorrelationId();
            ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "recovery");
            ByteBuffer resp = KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
            KafkaWireHelpers.skipResponseHeader(resp);
            List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
            assertFalse(entries.isEmpty());
        }
    }

    @Test
    @Timeout(15)
    @DisplayName("1.3 Malformed request: negative message_size triggers closure")
    void malformedNegativeMessageSize() throws Exception {
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            OutputStream out = s.getOutputStream();
            ByteBuffer bad = ByteBuffer.allocate(4);
            bad.putInt(-1);
            bad.flip();
            out.write(bad.array());
            out.flush();
            Thread.sleep(200);
        }
        try (Socket s2 = new Socket(brokerHost, brokerPort)) {
            int corrId = nextCorrelationId();
            ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "recovery");
            ByteBuffer resp = KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
            KafkaWireHelpers.skipResponseHeader(resp);
            List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
            assertFalse(entries.isEmpty());
        }
    }

    @Test
    @Timeout(15)
    @DisplayName("1.3 Malformed request: oversized client_id still works")
    void oversizedClientId() throws IOException {
        String hugeClientId = "x".repeat(4096);
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, hugeClientId);
        ByteBuffer resp = send(req);
        KafkaWireHelpers.skipResponseHeader(resp);
        List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
        assertFalse(entries.isEmpty());
    }

    @Test
    @Timeout(20)
    @DisplayName("1.4 Three concurrent clients connecting simultaneously")
    void threeConcurrentClients() throws Exception {
        int clientCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(clientCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < clientCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    int corrId = nextCorrelationId();
                    ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "concurrent");
                    ByteBuffer resp = KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
                    KafkaWireHelpers.skipResponseHeader(resp);
                    List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
                    if (!entries.isEmpty()) successCount.incrementAndGet();
                } catch (Exception e) {
                    // silent
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All clients should finish");
        assertEquals(clientCount, successCount.get());
    }

    @Test
    @Timeout(15)
    @DisplayName("1.5 Multiple sequential requests on same connection (pipelining)")
    void multipleSequentialRequestsOnSameConnection() throws IOException {
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            s.setTcpNoDelay(true);
            OutputStream out = s.getOutputStream();
            InputStream in = s.getInputStream();
            for (int i = 0; i < 5; i++) {
                int corrId = nextCorrelationId();
                ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "pipeline");
                byte[] reqBytes = new byte[req.remaining()];
                req.get(reqBytes);
                out.write(reqBytes);
                out.flush();

                // Read response
                byte[] sizeBytes = new byte[4];
                KafkaWireHelpers.readN(in, sizeBytes);
                int respSize = ByteBuffer.wrap(sizeBytes).getInt();
                byte[] respBytes = new byte[respSize];
                KafkaWireHelpers.readN(in, respBytes);
                ByteBuffer resp = ByteBuffer.wrap(respBytes);
                KafkaWireHelpers.skipResponseHeader(resp);
                // Parsing succeeded
            }
        }
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Invalid API key -- broker returns no response but stays alive")
    void invalidApiKey() throws Exception {
        // Send invalid API key, broker returns null so no response comes back
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            OutputStream out = s.getOutputStream();
            ByteBuffer body = ByteBuffer.allocate(0);
            ByteBuffer req = KafkaWireHelpers.frameRequest((short) 999, (short) 0,
                    nextCorrelationId(), "test", body);
            byte[] data = new byte[req.remaining()];
            req.get(data);
            out.write(data);
            out.flush();
        }
        // Verify broker is still alive
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "test");
        ByteBuffer resp = KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
        KafkaWireHelpers.skipResponseHeader(resp);
        List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
        assertFalse(entries.isEmpty(), "Broker should still be responsive");
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Client disconnects mid-request (partial write)")
    void clientDisconnectsMidRequest() throws Exception {
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            OutputStream out = s.getOutputStream();
            out.write(new byte[]{0, 0}); // 2 bytes only
            out.flush();
        }
        Thread.sleep(200);
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "after-disconnect");
        ByteBuffer resp = KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
        KafkaWireHelpers.skipResponseHeader(resp);
        List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
        assertFalse(entries.isEmpty(), "Broker should survive partial disconnect");
    }

    @Test
    @Timeout(15)
    @DisplayName("Edge: Corrupt message_size MAX_INT (huge)")
    void corruptMaxIntMessageSize() throws Exception {
        try (Socket s = new Socket(brokerHost, brokerPort)) {
            OutputStream out = s.getOutputStream();
            ByteBuffer bad = ByteBuffer.allocate(4);
            bad.putInt(Integer.MAX_VALUE);
            bad.flip();
            out.write(bad.array());
            out.flush();
        }
        Thread.sleep(200);
        int corrId = nextCorrelationId();
        ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "recovery");
        ByteBuffer resp = KafkaWireHelpers.sendAndReceive(brokerHost, brokerPort, req);
        KafkaWireHelpers.skipResponseHeader(resp);
        List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
        assertFalse(entries.isEmpty());
    }

    @Test
    @Timeout(60)
    @DisplayName("Edge: Multiple rapid sequential requests on different connections")
    void multipleRapidConnections() throws Exception {
        for (int i = 0; i < 10; i++) {
            int corrId = nextCorrelationId();
            ByteBuffer req = KafkaWireHelpers.buildApiVersionsRequest(corrId, "rapid-" + i);
            ByteBuffer resp = send(req);
            KafkaWireHelpers.skipResponseHeader(resp);
            List<short[]> entries = KafkaWireHelpers.parseApiVersionsBody(resp);
            assertFalse(entries.isEmpty(), "Request " + i + " should succeed");
        }
    }

    private void assertKeyRange(List<short[]> entries, short key, short min, short max) {
        for (short[] e : entries) {
            if (e[0] == key) {
                assertEquals(min, e[1], "minVersion for key " + key);
                assertEquals(max, e[2], "maxVersion for key " + key);
                return;
            }
        }
        fail("API key " + key + " not found in ApiVersions response");
    }
}
