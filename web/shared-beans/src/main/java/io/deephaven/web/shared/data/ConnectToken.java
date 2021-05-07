package io.deephaven.web.shared.data;

import java.io.Serializable;

public class ConnectToken implements Serializable {

    private byte[] bytes;

    public byte[] getBytes() {
        return bytes;
    }

    public ConnectToken setBytes(byte[] bob) {
        this.bytes = bob;
        return this;
    }

}
