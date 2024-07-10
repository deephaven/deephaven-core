//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ResettableWritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * An efficient reader for a Python table listener to extract columnar data based on the {@link RowSequence} in the
 * {@link TableUpdate}
 */
public class PythonTableDataReader {

    /**
     * Factory method for instance of {@link Context}
     *
     * @param chunkCapacity
     * @param columnSources
     * @return {@link Context}
     */
    public static io.deephaven.engine.table.Context makeContext(final int chunkCapacity,
            @NotNull final ColumnSource<?>... columnSources) {
        return new Context(chunkCapacity, columnSources);
    }

    /**
     * Create a context that can be reused to read all the chunks of a row sequence
     */
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

    /**
     * Copy data from a table by chunks into a 2D array
     *
     * @param context the context used in filling the output array
     * @param rowSeq indices of the rows of the table to put into the 2D array
     * @param columnSources columns of data to put into the 2D array
     * @return a 2D array
     */
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
