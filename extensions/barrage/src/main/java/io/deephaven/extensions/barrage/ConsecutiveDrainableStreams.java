//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.SizeException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;

public class ConsecutiveDrainableStreams extends DefensiveDrainable {
    final DefensiveDrainable[] streams;

    public ConsecutiveDrainableStreams(final @NotNull DefensiveDrainable... streams) {
        this.streams = streams;
    }

    @Override
    public int drainTo(final OutputStream outputStream) throws IOException {
        int total = 0;
        for (final DefensiveDrainable stream : streams) {
            final int expected = total + stream.available();
            total += stream.drainTo(outputStream);
            if (expected != total) {
                throw new IllegalStateException("drained message drained wrong number of bytes");
            }
            if (total < 0) {
                throw new IllegalStateException("drained message is too large; exceeds Integer.MAX_VALUE");
            }
        }
        return total;
    }

    @Override
    public int available() throws SizeException, IOException {
        int total = 0;
        for (final DefensiveDrainable stream : streams) {
            total += stream.available();
            if (total < 0) {
                throw new SizeException("drained message is too large; exceeds Integer.MAX_VALUE", total);
            }
        }
        return total;
    }

    @Override
    public void close() throws IOException {
        SafeCloseable.closeAll(streams);
        super.close();
    }
}
