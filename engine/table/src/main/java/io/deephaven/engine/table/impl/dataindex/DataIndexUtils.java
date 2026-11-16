//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortedColumnsAttribute;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToLongFunction;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * Tools for working with {@link DataIndex data indices}.
 */
public class DataIndexUtils {

    /**
     * Make a {@link ChunkSource} that produces data index {@link DataIndex.RowKeyLookup lookup} keys from
     * {@code keySources}.
     * 
     * @param keySources The individual key sources
     * @return The boxed key source
     */
    public static ChunkSource.WithPrev<Values> makeBoxedKeySource(@NotNull final ColumnSource<?>... keySources) {
        switch (keySources.length) {
            case 0:
                throw new IllegalArgumentException("Data index must have at least one key column");
            case 1:
                return new DataIndexBoxedKeySourceSingle(keySources[0]);
            default:
                return new DataIndexBoxedKeySourceCompound(keySources);
        }
    }

    /**
     * Make a {@link DataIndexKeySet} that stores data index {@link DataIndex.RowKeyLookup lookup} keys that have
     * {@code keyColumnCount} components.
     *
     * @param keyColumnCount The number of key components
     * @return The key set
     */
    public static DataIndexKeySet makeKeySet(final int keyColumnCount) {
        if (keyColumnCount == 1) {
            return new DataIndexKeySetSingle();
        }
        if (keyColumnCount > 1) {
            return new DataIndexKeySetCompound();
        }
        throw new IllegalArgumentException("Data index must have at least one key column");
    }

    /**
     * Make a {@link DataIndexKeySet} that stores data index {@link DataIndex.RowKeyLookup lookup} keys that have
     * {@code keyColumnCount} components.
     *
     * @param keyColumnCount The number of key components
     * @param initialCapacity The initial capacity
     * @return The key set
     */
    @SuppressWarnings("unused")
    public static DataIndexKeySet makeKeySet(final int keyColumnCount, final int initialCapacity) {
        if (keyColumnCount == 1) {
            return new DataIndexKeySetSingle(initialCapacity);
        }
        if (keyColumnCount > 1) {
            return new DataIndexKeySetCompound(initialCapacity);
        }
        throw new IllegalArgumentException("Data index must have at least one key column");
    }

    /**
     * Test equality between two data index {@link DataIndex.RowKeyLookup lookup} keys.
     *
     * @return Whether the two keys are equal
     */
    public static boolean lookupKeysEqual(@Nullable final Object key1, @Nullable final Object key2) {
        if (key1 instanceof Object[] && key2 instanceof Object[]) {
            return Arrays.equals((Object[]) key1, (Object[]) key2);
        }
        return Objects.equals(key1, key2);
    }

    /**
     * Compute the hash code for a data index {@link DataIndex.RowKeyLookup lookup} key.
     *
     * @param key The lookup key
     * @return The hash code
     */
    public static int hashLookupKey(@Nullable final Object key) {
        if (key instanceof Object[]) {
            return Arrays.hashCode((Object[]) key);
        }
        return Objects.hashCode(key);
    }

    /**
     * Implement {@link DataIndex.RowKeyLookup#apply(Chunk[], WritableLongChunk, boolean)} by boxing each key and
     * calling {@link DataIndex.RowKeyLookup#apply(Object, boolean)} on {@code lookup}, for implementations that cannot
     * search a chunk of keys at once.
     *
     * @param lookup The lookup to call for each key
     * @param keyChunks The lookup keys, one chunk per key column, at least one, all the same size
     * @param rowKeys Receives the result row key of each lookup key; its size is set to the number of keys
     * @param usePrev Whether to look up the keys in previous space
     */
    public static void lookupRowKeysBoxed(
            @NotNull final DataIndex.RowKeyLookup lookup,
            @NotNull final Chunk<? extends Values>[] keyChunks,
            @NotNull final WritableLongChunk<RowKeys> rowKeys,
            final boolean usePrev) {
        Require.gtZero(keyChunks.length, "keyChunks.length");
        final int size = keyChunks[0].size();
        rowKeys.setSize(size);
        // noinspection unchecked
        final ObjectChunk<?, ? extends Values>[] boxedKeys = new ObjectChunk[keyChunks.length];
        final ChunkBoxer.BoxerKernel[] boxers = new ChunkBoxer.BoxerKernel[keyChunks.length];
        try (final SafeCloseableArray<ChunkBoxer.BoxerKernel> ignored = new SafeCloseableArray<>(boxers)) {
            for (int ci = 0; ci < keyChunks.length; ++ci) {
                boxers[ci] = ChunkBoxer.getBoxer(keyChunks[ci].getChunkType(), size);
                boxedKeys[ci] = boxers[ci].box(keyChunks[ci]);
            }
            if (keyChunks.length == 1) {
                for (int ii = 0; ii < size; ++ii) {
                    rowKeys.set(ii, lookup.apply(boxedKeys[0].get(ii), usePrev));
                }
                return;
            }
            for (int ii = 0; ii < size; ++ii) {
                // A fresh array per key, since a lookup may keep the key it is given.
                final Object[] key = new Object[keyChunks.length];
                for (int ci = 0; ci < keyChunks.length; ++ci) {
                    key[ci] = boxedKeys[ci].get(ii);
                }
                rowKeys.set(ii, lookup.apply(key, usePrev));
            }
        }
    }

    /**
     * Present the key columns of an index table under the names a filter reads the key columns by, so that the filter
     * can be applied to the index table. Several filter columns may name one key column, as when a filter reads a
     * column and an alias of the column.
     * <p>
     * The result serves one filter operation and is then discarded, so the result is a static view with no listener,
     * built over the index table's own row set and column sources. The view does not follow the index table's updates,
     * yet shares the index table's row set and column sources, so the view drifts as the index table updates. Use the
     * view only within the operation that makes the view, and never publish the view. A caller that filters previous
     * values passes {@link io.deephaven.engine.table.impl.PrevTableTools#prevTable the index table's previous state} in
     * place of the index table, so the result presents the previous state under the filter's names.
     *
     * @param indexTable The index table
     * @param rowSetColumnName The name of the index table's row set column, which the result keeps
     * @param filterColumns The names of the columns the filter reads
     * @param renameMap A map from filter column name to key column name, for each filter column whose name differs from
     *        the name of the key column
     * @return {@code indexTable} itself if {@code indexTable} is static and the filter reads every key column under the
     *         key column's own name; otherwise a static view that holds each filter column under the filter's name, and
     *         the row set column
     */
    public static Table indexTableForFilter(
            @NotNull final Table indexTable,
            @NotNull final String rowSetColumnName,
            @NotNull final List<String> filterColumns,
            @NotNull final Map<String, String> renameMap) {
        if (renameMap.isEmpty() && !indexTable.isRefreshing()) {
            return indexTable;
        }
        final QueryTable source = (QueryTable) indexTable.coalesce();
        // Creating a new table lets one key column appear under several filter names, which renameColumns cannot do.
        final Map<String, ColumnSource<?>> columns = new LinkedHashMap<>(filterColumns.size() + 1);
        for (final String filterColumn : filterColumns) {
            columns.put(filterColumn, source.getColumnSource(renameMap.getOrDefault(filterColumn, filterColumn)));
        }
        columns.put(rowSetColumnName, source.getColumnSource(rowSetColumnName));
        final QueryTable result =
                new QueryTable(TableDefinition.inferFrom(source, columns), source.getRowSet(), columns);
        // The table carries no other attribute. A filter reads only the sort order, and the attribute copy rules key
        // the sort order by column name, which the table changes. So each filter name takes the order of the key
        // column the filter name reads, and every filter name reading a sorted key column is sorted the same way.
        for (final String filterColumn : filterColumns) {
            SortedColumnsAttribute.getOrderForColumn(source, renameMap.getOrDefault(filterColumn, filterColumn))
                    .ifPresent(order -> SortedColumnsAttribute.setOrderForColumn(result, filterColumn, order));
        }
        return result;
    }

    /**
     * Build a mapping function from the lookup keys of the provided index {@link Table} to row keys in the table.
     *
     * @param indexTable The {@link Table} to search
     * @param keyColumnNames The key columns to search
     * @return A mapping function from lookup keys to {@code indexTable} row keys
     */
    public static ToLongFunction<Object> buildRowKeyMappingFunction(
            final Table indexTable,
            final String[] keyColumnNames) {

        final KeyedObjectHashMap<Object, LookupKeyRowKeyPair> lookupKeyToRowKeyPairs =
                new KeyedObjectHashMap<>(LookupKeyRowKeyPair.KEYED_OBJECT_KEY);
        final ChunkSource.WithPrev<?> lookupKeySource = makeBoxedKeySource(Arrays.stream(keyColumnNames)
                .map(indexTable::getColumnSource)
                .toArray(ColumnSource[]::new));
        try (final CloseableIterator<Object> lookupKeyIterator =
                ChunkedColumnIterator.make(lookupKeySource, indexTable.getRowSet());
                final RowSet.Iterator rowKeyIterator = indexTable.getRowSet().iterator()) {
            while (lookupKeyIterator.hasNext()) {
                final Object lookupKey = lookupKeyIterator.next();
                final long rowKey = rowKeyIterator.nextLong();
                lookupKeyToRowKeyPairs.put(lookupKey, new LookupKeyRowKeyPair(lookupKey, rowKey));
            }
        }

        return (final Object lookupKey) -> {
            final LookupKeyRowKeyPair pair = lookupKeyToRowKeyPairs.get(lookupKey);
            return pair == null ? NULL_ROW_KEY : pair.rowKey;
        };
    }

    private static final class LookupKeyRowKeyPair {

        private static final KeyedObjectKey<Object, LookupKeyRowKeyPair> KEYED_OBJECT_KEY =
                new LookupKeyedObjectKey<>() {

                    @Override
                    public Object getKey(@NotNull final LookupKeyRowKeyPair lookupKeyRowKeyPair) {
                        return lookupKeyRowKeyPair.lookupKey;
                    }
                };

        private final Object lookupKey;
        private final long rowKey;

        private LookupKeyRowKeyPair(final Object lookupKey, final long rowKey) {
            this.lookupKey = lookupKey;
            this.rowKey = rowKey;
        }
    }

    /**
     * A {@link KeyedObjectKey} that for values keyed by a lookup key.
     */
    public static abstract class LookupKeyedObjectKey<VALUE_TYPE> implements KeyedObjectKey<Object, VALUE_TYPE> {

        @Override
        public int hashKey(final Object key) {
            return hashLookupKey(key);
        }

        @Override
        public boolean equalKey(final Object key, final VALUE_TYPE value) {
            return lookupKeysEqual(key, getKey(value));
        }
    }
}
