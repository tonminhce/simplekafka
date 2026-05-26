package com.simplekafka.broker;

/**
 * Immutable value object representing a broker's identity.
 */
public class BrokerInfo {

    private final int brokerId;
    private final String host;
    private final int port;

    public BrokerInfo(int brokerId, String host, int port) {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }
}
