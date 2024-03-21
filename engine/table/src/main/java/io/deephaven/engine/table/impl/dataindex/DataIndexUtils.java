//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
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
