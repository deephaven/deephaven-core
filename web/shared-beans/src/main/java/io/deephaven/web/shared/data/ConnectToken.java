/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
