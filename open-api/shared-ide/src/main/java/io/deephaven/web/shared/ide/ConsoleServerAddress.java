package io.deephaven.web.shared.ide;

import jsinterop.annotations.JsProperty;

import java.io.Serializable;

/**
 */
public class ConsoleServerAddress implements Serializable {

    private String host;
    private String name;
    private int port;

    @JsProperty
    public String getHost() {
        return host;
    }

    public ConsoleServerAddress setHost(String host) {
        this.host = host;
        return this;
    }

    @JsProperty
    public String getName() {
        return name;
    }

    public ConsoleServerAddress setName(String name) {
        this.name = name;
        return this;
    }

    @JsProperty
    public int getPort() {
        return port;
    }

    public ConsoleServerAddress setPort(int port) {
        this.port = port;
        return this;
    }
}
