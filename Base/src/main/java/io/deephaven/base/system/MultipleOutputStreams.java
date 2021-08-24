package io.deephaven.base.system;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

final class MultipleOutputStreams extends OutputStream {

    public static OutputStream of(List<OutputStream> streams) {
        if (streams.size() == 1) {
            return streams.get(0);
        }
        return new MultipleOutputStreams(streams);
    }

    private final List<OutputStream> outputs;

    MultipleOutputStreams(List<OutputStream> outputs) {
        this.outputs = Objects.requireNonNull(outputs);
    }

    @Override
    public synchronized void write(int b) throws IOException {
        for (OutputStream output : outputs) {
            output.write(b);
        }
    }

    @Override
    public synchronized void write(@NotNull byte[] b) throws IOException {
        for (OutputStream output : outputs) {
            output.write(b);
        }
    }

    @Override
    public synchronized void write(@NotNull byte[] b, int off, int len) throws IOException {
        for (OutputStream output : outputs) {
            output.write(b, off, len);
        }
    }

    @Override
    public synchronized void flush() throws IOException {
        for (OutputStream output : outputs) {
            output.flush();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        close(outputs.iterator());
    }

    private static void close(Iterator<OutputStream> it) throws IOException {
        if (it.hasNext()) {
            OutputStream next = it.next();
            try {
                next.close();
            } finally {
                close(it);
            }
        }
    }
}
