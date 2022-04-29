package io.deephaven.integrations.python;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;

public class PythonListenerTableUpdateDataReader {


    public static io.deephaven.engine.table.Context makeContext(final int chunkCapacity,
            @NotNull final ColumnSource<?>... columnSources) {
        return new Context(chunkCapacity, columnSources);
    }

    private static class Context implements io.deephaven.engine.table.Context {

        private final SharedContext sharedContext;
        private final ChunkSource.FillContext[] fillContexts;
        private final ResettableWritableChunk<Values>[] resettableChunks;

        private Context(final int chunkCapacity, @NotNull final ColumnSource<?>... columnSources) {
            sharedContext = SharedContext.makeSharedContext();
            fillContexts = Arrays.stream(columnSources).map(cs -> cs.makeFillContext(chunkCapacity, sharedContext))
                    .toArray(ChunkSource.FillContext[]::new);
            // noinspection unchecked
            resettableChunks =
                    Arrays.stream(columnSources).map(cs -> cs.getChunkType().<Values>makeResettableWritableChunk())
                            .toArray(ResettableWritableChunk[]::new);
        }

        @Override
        public void close() {
            sharedContext.close();
            for (final SafeCloseable closeable : fillContexts) {
                closeable.close();
            }
            for (final SafeCloseable closeable : resettableChunks) {
                closeable.close();
            }
        }
    }

    public static Object[] readChunkColumnMajor(@NotNull final io.deephaven.engine.table.Context context,
            final RowSequence rowSeq,
            final ColumnSource<?>[] columnSources, final boolean prev) {
        final int nRows = rowSeq.intSize();
        final int nCols = columnSources.length;
        final Object[] arrays = Arrays.stream(columnSources).map(cs -> {
            final ChunkType chunkType = cs.getChunkType();
            if (chunkType == ChunkType.Object) {
                return Array.newInstance(cs.getType(), rowSeq.intSize());
            }
            return cs.getChunkType().makeArray(rowSeq.intSize());
        }).toArray(Object[]::new);
        final Context typedContext = (Context) context;

        for (int ci = 0; ci < nCols; ++ci) {
            final ChunkSource.FillContext fillContext = typedContext.fillContexts[ci];
            final ResettableWritableChunk<Values> chunk = typedContext.resettableChunks[ci];
            final Object array = arrays[ci];
            chunk.resetFromArray(array, 0, nRows);
            final ColumnSource<?> colSrc = columnSources[ci];

            // noinspection unchecked
            if (prev) {
                colSrc.fillPrevChunk(fillContext, chunk, rowSeq);
            } else {
                colSrc.fillChunk(fillContext, chunk, rowSeq);
            }
            chunk.clear();
        }
        typedContext.sharedContext.reset();

        return arrays;
    }
}
