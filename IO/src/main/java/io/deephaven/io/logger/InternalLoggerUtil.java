package io.deephaven.io.logger;

import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogSink;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Class for logger utilities for internal Logger usage. Not named LoggerUtil or LoggerUtils to
 * avoid confusion with those existing classes.
 */
class InternalLoggerUtil {

    /**
     * Write the specified LogEntry to the specified stream flipping buffers, then call any
     * interceptors.
     * 
     * @param entry the entry to be written
     * @param outputStream the OutputStream to which the entry is written
     * @param interceptors interceptors to be called
     * @throws IOException from the outputStream write operations
     */
    static void writeEntryToStream(final LogEntry entry, final OutputStream outputStream,
        final LogSink.Interceptor[] interceptors) throws IOException {
        for (int i = 0; i < entry.getBufferCount(); ++i) {
            final ByteBuffer b = entry.getBuffer(i);
            b.flip();
        }

        if (interceptors != null) {
            for (final LogSink.Interceptor interceptor : interceptors) {
                // noinspection unchecked
                interceptor.element(entry, entry);
            }
        }

        // noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (outputStream) {
            for (int i = 0; i < entry.getBufferCount(); ++i) {
                final ByteBuffer b = entry.getBuffer(i);
                if (b.hasArray()) {
                    final byte[] ba = b.array();
                    outputStream.write(ba, 0, b.limit()); // Doesn't reset buffer position and limit
                } else {
                    final int initialPosition = b.position();
                    final int initialLimit = b.limit();
                    while (b.remaining() > 0) { // This loop will reset buffer position and limit
                        outputStream.write(b.get());
                    }
                    b.limit(initialLimit).position(initialPosition);
                }
            }
        }
    }
}
