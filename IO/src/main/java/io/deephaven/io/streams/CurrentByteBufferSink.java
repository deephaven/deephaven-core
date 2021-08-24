package io.deephaven.io.streams;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A ByteBufferSink that knows and provides a getter for the last buffer it gave out, to avoid
 * unnecessary state in code that uses the buffer.
 */
public interface CurrentByteBufferSink extends ByteBufferSink {

    /**
     * Access the current buffer for this sink. This is either the initial buffer, or the last one
     * provided by {@link ByteBufferSink#acceptBuffer(ByteBuffer, int)}) or
     * {@link CurrentByteBufferSink#ensureSpace(int)}.
     * 
     * @return The current buffer for this sink
     */
    ByteBuffer getBuffer();

    /**
     * Return the current buffer, guaranteed to have sufficient space remaining to append the
     * requested number of bytes. The existing current buffer may be accepted (see
     * {@link ByteBufferSink#acceptBuffer(ByteBuffer, int)}) as a side effect of this operation.
     * 
     * @param need The number of bytes required to proceed
     * @return The current buffer for further output
     */
    default ByteBuffer ensureSpace(final int need) throws IOException {
        final ByteBuffer current = getBuffer();
        if (current.remaining() < need) {
            // Note that acceptBuffer is expected to flip() the buffer.
            return acceptBuffer(current, need);
        }
        return current;
    }

    /**
     * Cause the current buffer to be accepted if it has any contents that aren't yet accepted into
     * the sink.
     */
    default void flush() throws IOException {
        final ByteBuffer current = getBuffer();
        if (current.position() != 0) {
            acceptBuffer(current, 0);
        }
    }

    /**
     * Convenience close method. Effectively the same as invoking
     * {@link ByteBufferSink#close(ByteBuffer)} with the result of
     * {@link CurrentByteBufferSink#getBuffer()}.
     */
    default void close() throws IOException {
        close(getBuffer());
    }

    class Adapter implements CurrentByteBufferSink {

        private final ByteBufferSink innerSink;

        private ByteBuffer current;

        public Adapter(@NotNull final ByteBufferSink innerSink,
            @NotNull final ByteBuffer initialBuffer) {
            this.innerSink = Require.neqNull(innerSink, "innerSink");
            this.current = Require.neqNull(initialBuffer, "initialBuffer");
        }

        @Override
        public ByteBuffer getBuffer() {
            return current;
        }

        @Override
        public ByteBuffer acceptBuffer(@NotNull final ByteBuffer buffer, final int need)
            throws IOException {
            if (buffer != current) {
                throw new UnsupportedOperationException(
                    "Expected current buffer " + current + ", instead tried to accept " + buffer);
            }
            return current = innerSink.acceptBuffer(current, need);
        }

        @Override
        public void close(@NotNull final ByteBuffer buffer) throws IOException {
            if (buffer != current) {
                throw new UnsupportedOperationException("Expected current buffer " + current
                    + ", instead tried to close with " + buffer);
            }
            innerSink.close(current);
            current = null;
        }
    }
}
