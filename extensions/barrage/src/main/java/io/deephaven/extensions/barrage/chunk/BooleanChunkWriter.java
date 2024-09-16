//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

import static io.deephaven.util.QueryConstants.*;

public class BooleanChunkWriter extends BaseChunkWriter<ByteChunk<Values>> {
    private static final String DEBUG_NAME = "BooleanChunkWriter";
    public static final BooleanChunkWriter INSTANCE = new BooleanChunkWriter();

    public BooleanChunkWriter() {
        super(ByteChunk::isNull, ByteChunk::getEmptyChunk, 0, false);
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context<ByteChunk<Values>> context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new BooleanChunkInputStream(context, subset, options);
    }

    private class BooleanChunkInputStream extends BaseChunkInputStream<Context<ByteChunk<Values>>> {
        private BooleanChunkInputStream(
                @NotNull final Context<ByteChunk<Values>> context,
                @Nullable final RowSet subset,
                @NotNull final BarrageOptions options) {
            super(context, subset, options);
        }

        @Override
        protected int getRawSize() {
            long size = 0;
            if (sendValidityBuffer()) {
                size += getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME));
            }
            size += getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES;
            return LongSizedDataStructure.intSize(DEBUG_NAME, size);
        }

        @Override
        public void visitFieldNodes(final FieldNodeListener listener) {
            listener.noteLogicalFieldNode(subset.intSize(DEBUG_NAME), nullCount());
        }

        @Override
        public void visitBuffers(final BufferListener listener) {
            // validity
            int validityLen = sendValidityBuffer() ? getValidityMapSerializationSizeFor(subset.intSize(DEBUG_NAME)) : 0;
            listener.noteLogicalBuffer(validityLen);
            // payload
            listener.noteLogicalBuffer(getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES);
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            if (read || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            read = true;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten += writeValidityBuffer(dos);

            // write the payload buffer
            final SerContext serContext = new SerContext();
            final Runnable flush = () -> {
                try {
                    dos.writeLong(serContext.accumulator);
                } catch (final IOException e) {
                    throw new UncheckedDeephavenException("Unexpected exception while draining data to OutputStream: ",
                            e);
                }
                serContext.accumulator = 0;
                serContext.count = 0;
            };

            subset.forAllRowKeys(row -> {
                final byte byteValue = context.getChunk().get((int) row);
                if (byteValue != NULL_BYTE) {
                    serContext.accumulator |= (byteValue > 0 ? 1L : 0L) << serContext.count;
                }
                if (++serContext.count == 64) {
                    flush.run();
                }
            });
            if (serContext.count > 0) {
                flush.run();
            }
            bytesWritten += getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES;

            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
