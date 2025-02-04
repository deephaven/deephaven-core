//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A {@link ChunkWriter} implementation that writes an Apache Arrow Null Column; which only writes a field node.
 */
public class NullChunkWriter extends BaseChunkWriter<Chunk<Values>> {
    private static final String DEBUG_NAME = "NullChunkWriter";

    public static final NullChunkWriter INSTANCE = new NullChunkWriter();

    public NullChunkWriter() {
        super(null, () -> null, 0, true, true);
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context chunk,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new NullDrainableColumn(subset == null ? chunk.size() : subset.intSize(DEBUG_NAME));
    }

    @Override
    protected int computeNullCount(@NotNull final Context context, @NotNull final RowSequence subset) {
        return subset.intSize("NullChunkWriter");
    }

    @Override
    protected void writeValidityBufferInternal(
            @NotNull final Context context,
            @NotNull final RowSequence subset,
            @NotNull final SerContext serContext) {
        // nothing to do; this is a null column
    }

    public static class NullDrainableColumn extends DrainableColumn {
        private final int size;

        public NullDrainableColumn(int size) {
            this.size = size;
        }

        @Override
        public void visitFieldNodes(FieldNodeListener listener) {
            listener.noteLogicalFieldNode(size, size);
        }

        @Override
        public void visitBuffers(BufferListener listener) {
            // there are no buffers for null columns
        }

        @Override
        public int nullCount() {
            return size;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            // we only write the field node, so there is nothing to drain
            return 0;
        }

        @Override
        public int available() throws IOException {
            // we only write the field node, so there is no data available
            return 0;
        }
    }
}
