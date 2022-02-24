/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkInputStreamGenerator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.chunk;

import gnu.trove.iterator.TLongIterator;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

import static io.deephaven.util.QueryConstants.*;

public class IntChunkInputStreamGenerator extends BaseChunkInputStreamGenerator<IntChunk<Values>> {
    private static final String DEBUG_NAME = "IntChunkInputStreamGenerator";

    IntChunkInputStreamGenerator(final IntChunk<Values> chunk, final int elementSize) {
        super(chunk, elementSize);
    }

    @Override
    public DrainableColumn getInputStream(final StreamReaderOptions options, final @Nullable RowSet subset) {
        return new IntChunkInputStream(options, subset);
    }

    private class IntChunkInputStream extends BaseChunkInputStream {
        private IntChunkInputStream(final StreamReaderOptions options, final RowSet subset) {
            super(chunk, options, subset);
        }

        private int cachedNullCount = - 1;

        @Override
        public int nullCount() {
            if (options.useDeephavenNulls()) {
                return 0;
            }
            if (cachedNullCount == -1) {
                cachedNullCount = 0;
                subset.forAllRowKeys(row -> {
                    if (chunk.get((int) row) == NULL_INT) {
                        ++cachedNullCount;
                    }
                });
            }
            return cachedNullCount;
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            listener.noteLogicalBuffer(sendValidityBuffer() ? getValidityMapSerializationSizeFor(subset.intSize()) : 0);
            // payload
            long length = elementSize * subset.size();
            final long bytesExtended = length & REMAINDER_MOD_8_MASK;
            if (bytesExtended > 0) {
                length += 8 - bytesExtended;
            }
            listener.noteLogicalBuffer(length);
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            read = true;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);
            // write the validity array with LSB indexing
            if (sendValidityBuffer()) {
                final SerContext context = new SerContext();
                final Runnable flush = () -> {
                    try {
                        dos.writeLong(context.accumulator);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException("Unexpected exception while draining data to OutputStream: ", e);
                    }
                    context.accumulator = 0;
                    context.count = 0;
                };
                subset.forAllRowKeys(row -> {
                    if (chunk.get((int) row) != NULL_INT) {
                        context.accumulator |= 1L << context.count;
                    }
                    if (++context.count == 64) {
                        flush.run();
                    }
                });
                if (context.count > 0) {
                    flush.run();
                }

                bytesWritten += getValidityMapSerializationSizeFor(subset.intSize());
            }

                // write the included values
                subset.forAllRowKeys(row -> {
                    try {
                        final int val = chunk.get((int) row);
                        dos.writeInt(val);
                    } catch (final IOException e) {
                        throw new UncheckedDeephavenException("Unexpected exception while draining data to OutputStream: ", e);
                    }
                });

                bytesWritten += elementSize * subset.size();
                final long bytesExtended = bytesWritten & REMAINDER_MOD_8_MASK;
                if (bytesExtended > 0) {
                    bytesWritten += 8 - bytesExtended;
                    dos.write(PADDING_BUFFER, 0, (int) (8 - bytesExtended));
                }
            return LongSizedDataStructure.intSize("IntChunkInputStreamGenerator", bytesWritten);
        }
    }

    @FunctionalInterface
    public interface IntConversion {
        int apply(int in);
        IntConversion IDENTITY = (int a) -> a;
    }

    static Chunk<Values> extractChunkFromInputStream(
            final int elementSize,
            final StreamReaderOptions options,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is) throws IOException {
        return extractChunkFromInputStreamWithConversion(
                elementSize, options, IntConversion.IDENTITY, fieldNodeIter, bufferInfoIter, is);
    }

    static Chunk<Values> extractChunkFromInputStreamWithConversion(
            final int elementSize,
            final StreamReaderOptions options,
            final IntConversion conversion,
            final Iterator<FieldNodeInfo> fieldNodeIter,
            final TLongIterator bufferInfoIter,
            final DataInput is) throws IOException {

        final FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.next();
        final long payloadBuffer = bufferInfoIter.next();

        final WritableIntChunk<Values> chunk = WritableIntChunk.makeWritableChunk(nodeInfo.numElements);

        if (nodeInfo.numElements == 0) {
            return chunk;
        }

        final int numValidityLongs = options.useDeephavenNulls() ? 0 : (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
            if (options.useDeephavenNulls() && validityBuffer != 0) {
                throw new IllegalStateException("validity buffer is non-empty, but is unnecessary");
            }
            int jj = 0;
            for (; jj < Math.min(numValidityLongs, validityBuffer / 8); ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L;
            if (valBufRead < validityBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityLongs; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }
            // consumed entire validity buffer by here

            final long payloadRead = (long) nodeInfo.numElements * elementSize;
            if (payloadBuffer < payloadRead) {
                throw new IllegalStateException("payload buffer is too short for expected number of elements");
            }

            if (options.useDeephavenNulls()) {
                if (conversion == IntChunkInputStreamGenerator.IntConversion.IDENTITY) {
                    for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                        chunk.set(ii, is.readInt());
                    }
                } else {
                    for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                        final int in = is.readInt();
                        final int out;
                        if (in == NULL_INT) {
                            out = in;
                        } else {
                            out = conversion.apply(in);
                        }
                        chunk.set(ii, out);
                    }
                }
            } else {
                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    if ((ii % 64) == 0) {
                        nextValid = isValid.get(ii / 64);
                    }
                    final int value;
                    if ((nextValid & 0x1) == 0x0) {
                        value = NULL_INT;
                        is.skipBytes(elementSize);
                    } else {
                        value = conversion.apply(is.readInt());
                    }
                    nextValid >>= 1;
                    chunk.set(ii, value);
                }
            }

            final long overhangPayload = payloadBuffer - payloadRead;
            if (overhangPayload > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, overhangPayload));
            }
        }

        chunk.setSize(nodeInfo.numElements);
        return chunk;
    }
}
