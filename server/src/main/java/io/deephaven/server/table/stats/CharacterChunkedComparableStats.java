package io.deephaven.server.table.stats;

import gnu.trove.map.hash.TObjectLongHashMap;
import io.deephaven.chunk.CharChunk;
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

public class CharacterChunkedComparableStats implements ChunkedComparableStatsKernel<Character> {
    private static final int MAX_UNIQUES = 1_000_000;
    @Override
    public Table processChunks(final RowSet index, final ColumnSource<?> columnSource, boolean usePrev, int maxUnique) {
        long count = 0;
        int uniqueCount = 0;

        final TObjectLongHashMap<Comparable<?>> countValues = new TObjectLongHashMap<>();
        boolean useSet = false;
        final Set<Comparable<?>> uniqueValues = new HashSet<>();

        try (final ChunkSource.GetContext getContext = columnSource.makeGetContext(CHUNK_SIZE)) {
            final RowSequence.Iterator okIt = index.getRowSequenceIterator();

            while (okIt.hasMore()) {
                // Grab up to the next CHUNK_SIZE rows
                final RowSequence nextKeys = okIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final CharChunk<? extends Values> chunk = (usePrev ? columnSource.getPrevChunk(getContext, nextKeys)
                        : columnSource.getChunk(getContext, nextKeys)).asCharChunk();
                final int chunkSize = chunk.size();
                for (int ii = 0; ii < chunkSize; ii++) {
                    final char val = chunk.get(ii);

                    if (val == QueryConstants.NULL_CHAR) {
                        continue;
                    }

                    count++;

                    if (useSet) {
                        uniqueValues.add((Comparable<?>) val);
                    } else if (uniqueCount > MAX_UNIQUES) {
                        // we no longer need to track counts for these items; fall back to a Set to get at least a count
                        uniqueValues.addAll(countValues.keySet());
                        countValues.clear();
                        uniqueValues.add((Comparable<?>) val);
                        useSet = true;
                    } else if (countValues.adjustOrPutValue((Comparable<?>) val, 1, 1) == 1) {
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
                TreeSet<Map.Entry<String, Long>> sorted = new TreeSet<>(Map.Entry.comparingByValue());
                countValues.forEachEntry((o, c) -> {
                    sorted.add(Map.entry(Objects.toString(o), c));
                    return true;
                });

                int resultCount = Math.min(maxUnique, sorted.size());
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
                    TableTools.longCol("SIZE", index.size()),
                    TableTools.intCol("UNIQUE_VALUES", numUnique),
                    new ColumnHolder<>("UNIQUE_KEYS", String[].class, String.class, false, uniqueKeys),
                    new ColumnHolder<>("UNIQUE_COUNTS", long[].class, long.class, false, uniqueCounts));
        }
    }
}
