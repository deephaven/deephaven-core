package io.deephaven.base.text;

import java.nio.ByteBuffer;

public class TimestampBuffer {
    public ByteBuffer getTimestamp(long nowMillis) {
        throw new UnsupportedOperationException("TimestampBuffer is not supported in the web client API");
    }
}
