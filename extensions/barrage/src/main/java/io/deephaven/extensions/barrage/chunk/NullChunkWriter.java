//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageOptions;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;

public class NullChunkWriter<SourceChunkType extends Chunk<Values>> extends BaseChunkWriter<SourceChunkType> {
    public static final NullChunkWriter<Chunk<Values>> INSTANCE = new NullChunkWriter<>();

    public NullChunkWriter() {
        super((chunk, idx) -> true, () -> null, 0, true, true);
    }

    @Override
    public DrainableColumn getInputStream(
            @NotNull final Context<SourceChunkType> chunk,
            @Nullable final RowSet subset,
            @NotNull final BarrageOptions options) throws IOException {
        return new NullDrainableColumn();
    }

    public static class NullDrainableColumn extends DrainableColumn {

        @Override
        public void visitFieldNodes(FieldNodeListener listener) {}

        @Override
        public void visitBuffers(BufferListener listener) {}

        @Override
        public int nullCount() {
            return 0;
        }

        @Override
        public int drainTo(final OutputStream outputStream) throws IOException {
            return 0;
        }

        @Override
        public int available() throws IOException {
            return 0;
        }
    }
}
