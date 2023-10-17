package io.deephaven.server.table.stats;

import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;

import java.util.Set;

public class ObjectChunkedStats implements ChunkedComparableStatsKernel<Object> {

    @Override
    public Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev, int maxUnique) {
        long count = 0;
        int uniqueCount = 0;

        // TODO do we really not want to do equals/hashcode to collect a few (frequent) unique values?
        final TObjectLongHashMap<Comparable<?>> countValues = null;
        boolean useSet = false;
        final Set<Comparable<?>> uniqueValues = null;

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                // Grab up to the next CHUNK_SIZE rows
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final ObjectChunk<?, ? extends Values> chunk =
                        (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                                : columnSource.getChunk(getContext, nextKeys)).asObjectChunk();
                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final Object val = chunk.get(ii);

                    if (val == null) {
                        continue;
                    }

                    count++;

                }
            }

            return TableTools.newTable(
                    TableTools.longCol("COUNT", count),
                    TableTools.longCol("SIZE", index.size()));
        }
    }
}
