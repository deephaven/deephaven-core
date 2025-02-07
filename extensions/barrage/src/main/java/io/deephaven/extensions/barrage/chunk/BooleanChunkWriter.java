//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import com.google.common.io.LittleEndianDataOutputStream;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

public class BooleanChunkWriter extends BaseChunkWriter<ByteChunk<Values>> {
    private static final String DEBUG_NAME = "BooleanChunkWriter";
    private static final BooleanChunkWriter NULLABLE_IDENTITY_INSTANCE = new BooleanChunkWriter(true);
    private static final BooleanChunkWriter NON_NULLABLE_IDENTITY_INSTANCE = new BooleanChunkWriter(false);

    public static BooleanChunkWriter getIdentity(boolean isNullable) {
        return isNullable ? NULLABLE_IDENTITY_INSTANCE : NON_NULLABLE_IDENTITY_INSTANCE;
    }

    private BooleanChunkWriter(final boolean isNullable) {
        super(null, ByteChunk::getEmptyChunk, 0, false, isNullable);
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context context,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new BooleanChunkInputStream(context, subset, options);
    }

    @Override
    protected int computeNullCount(@NotNull Context context, @NotNull RowSequence subset) {
        final MutableInt nullCount = new MutableInt(0);
        final ByteChunk<Values> byteChunk = context.getChunk().asByteChunk();
        subset.forAllRowKeys(row -> {
            if (BooleanUtils.isNull(byteChunk.get((int) row))) {
                nullCount.increment();
            }
        });
        return nullCount.get();
    }

    @Override
    protected void writeValidityBufferInternal(@NotNull Context context, @NotNull RowSequence subset,
            @NotNull SerContext serContext) {
        final ByteChunk<Values> byteChunk = context.getChunk().asByteChunk();
        subset.forAllRowKeys(row -> serContext.setNextIsNull(BooleanUtils.isNull(byteChunk.get((int) row))));
    }

    private class BooleanChunkInputStream extends BaseChunkInputStream<Context> {
        private BooleanChunkInputStream(
                @NotNull final Context context,
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
            if (hasBeenRead || subset.isEmpty()) {
                return 0;
            }

            long bytesWritten = 0;
            hasBeenRead = true;
            final LittleEndianDataOutputStream dos = new LittleEndianDataOutputStream(outputStream);

            // write the validity buffer
            bytesWritten += writeValidityBuffer(dos);

            // write the payload buffer
            // we cheat and re-use validity buffer serialization code
            try (final SerContext serContext = new SerContext(dos)) {
                final ByteChunk<Values> byteChunk = context.getChunk().asByteChunk();
                subset.forAllRowKeys(row -> serContext.setNextIsNull(
                        BooleanUtils.byteAsBoolean(byteChunk.get((int) row)) != Boolean.TRUE));
            }
            bytesWritten += getNumLongsForBitPackOfSize(subset.intSize(DEBUG_NAME)) * (long) Long.BYTES;

            return LongSizedDataStructure.intSize(DEBUG_NAME, bytesWritten);
        }
    }
}
