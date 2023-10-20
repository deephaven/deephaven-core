/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterChunkedStats and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.server.table.stats;

import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ObjectChunkedStats implements ChunkedStatsKernel {
    private final int maxUniqueToCollect;
    private final int maxUniqueToDisplay;

    public ObjectChunkedStats(int maxUniqueToCollect, int maxUniqueToDisplay) {
        this.maxUniqueToCollect = maxUniqueToCollect;
        this.maxUniqueToDisplay = maxUniqueToDisplay;
    }

    @Override
    public Table processChunks(final RowSet rowSet, final ColumnSource<?> columnSource, boolean usePrev) {
        long count = 0;
        int uniqueCount = 0;

        final TObjectLongHashMap<Object> countValues = new TObjectLongHashMap<>();
        boolean useSet = false;
        final Set<Object> uniqueValues = new HashSet<>();

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator rsIt = rowSet.getRowSequenceIterator();

            while (rsIt.hasMore()) {
                // Grab up to the next CHUNK_SIZE rows
                final RowSequence nextKeys = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);

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

                    if (useSet) {
                        uniqueValues.add(val);
                    } else if (uniqueCount > maxUniqueToCollect) {
                        // we no longer need to track counts for these items; fall back to a Set to get at least a count
                        uniqueValues.addAll(countValues.keySet());
                        countValues.clear();
                        uniqueValues.add(val);
                        useSet = true;
                    } else if (countValues.adjustOrPutValue(val, 1, 1) == 1) {
                        uniqueCount++;
                    }
                }
            }

            final int numUnique;
            String[] uniqueKeys;
            long[] uniqueCounts;
            if (useSet) {
                numUnique = uniqueValues.size();
                uniqueKeys = CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
                uniqueCounts = CollectionUtil.ZERO_LENGTH_LONG_ARRAY;
            } else {
                numUnique = countValues.size();
                List<Map.Entry<String, Long>> sorted = new ArrayList<>();

                countValues.forEachEntry((o, c) -> {
                    sorted.add(Map.entry(Objects.toString(o), c));
                    return true;
                });
                sorted.sort(Map.Entry.comparingByValue());

                int resultCount = Math.min(maxUniqueToDisplay, sorted.size());
                uniqueKeys = new String[resultCount];
                uniqueCounts = new long[resultCount];
                Iterator<Map.Entry<String, Long>> iter = sorted.iterator();
                for (int i = 0; i < resultCount && iter.hasNext(); i++) {
                    Map.Entry<String, Long> entry = iter.next();
                    uniqueKeys[i] = entry.getKey();
                    uniqueCounts[i] = entry.getValue();
                }
            }

            return TableTools.newTable(
                    TableTools.longCol("COUNT", count),
                    TableTools.longCol("SIZE", rowSet.size()),
                    TableTools.intCol("UNIQUE_VALUES", numUnique),
                    new ColumnHolder<>("UNIQUE_KEYS", String[].class, String.class, false, uniqueKeys),
                    new ColumnHolder<>("UNIQUE_COUNTS", long[].class, long.class, false, uniqueCounts));
        }
    }
}
