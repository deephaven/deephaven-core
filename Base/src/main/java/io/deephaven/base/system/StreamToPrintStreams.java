/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base.system;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Optional;

/**
 * An implementation that delegates to {@link PrintStream print streams}.
 */
public class StreamToPrintStreams implements StandardStreamReceiver {

    private final PrintStream out;
    private final PrintStream err;

    /**
     * Construct a new instance
     *
     * @param out optional out stream
     * @param err optional err stream
     */
    public StreamToPrintStreams(PrintStream out, PrintStream err) {
        this.out = out;
        this.err = err;
    }

    @Override
    public Optional<OutputStream> receiveOut() {
        return Optional.ofNullable(out);
    }

    @Override
    public Optional<OutputStream> receiveErr() {
        return Optional.ofNullable(err);
    }
}
