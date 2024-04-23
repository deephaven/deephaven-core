//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pagestore.topage;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.stringset.LongBitmapStringSet;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.datastructures.SoftCachingSupplier;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Chunk-backed dictionary for use by {@link ToPage} implementations.
 */
public class ChunkDictionary<T, ATTR extends Any> implements LongBitmapStringSet.ReversibleLookup<T> {

    @FunctionalInterface
    public interface Lookup<T> {

        /**
         * Apply whatever lookup logic needs to be applied to a "raw" {@link Dictionary} in order to get a value for the
         * supplied {@code key}.
         *
         * @param dictionary The {@link Dictionary}
         * @param key The key
         * @return The value that should be mapped by the enclosing {@link ChunkDictionary}
         */
        T lookup(@NotNull final Dictionary dictionary, final int key);
    }

    private final Supplier<ObjectChunk<T, ATTR>> valuesSupplier;
    private final Supplier<TObjectIntMap<T>> reverseMapSupplier;

    /**
     * Construct a ChunkDictionary with the supplied {@link Lookup} function and {@link Dictionary} supplier
     *
     * @param lookup Value {@link Lookup} function
     * @param dictionarySupplier {@link Dictionary} supplier
     */
    ChunkDictionary(
            @NotNull final Lookup<T> lookup,
            @NotNull final Function<SeekableChannelContext, Dictionary> dictionarySupplier) {
        this.valuesSupplier = new SoftCachingSupplier<>(() -> {
            // We use NULL channel context here and rely on materialization logic to provide the correct context
            final Dictionary dictionary = dictionarySupplier.apply(SeekableChannelContext.NULL);
            final T[] values = ObjectChunk.makeArray(dictionary.getMaxId() + 1);
            for (int ki = 0; ki < values.length; ++ki) {
                values[ki] = lookup.lookup(dictionary, ki);
            }
            return ObjectChunk.chunkWrap(values);
        });
        this.reverseMapSupplier = new SoftCachingSupplier<>(() -> {
            final ObjectChunk<T, ATTR> values = getChunk();
            final TObjectIntMap<T> reverseMap = new TObjectIntHashMap<>(values.size());
            for (int vi = 0; vi < values.size(); ++vi) {
                reverseMap.put(values.get(vi), vi);
            }
            return reverseMap;
        });
    }

    ObjectChunk<T, ATTR> getChunk() {
        return valuesSupplier.get();
    }

    @Override
    public final T get(final long index) {
        final ObjectChunk<T, ATTR> values = getChunk();
        return index < 0 || index >= values.size() ? null : values.get((int) index);
    }

    @Override
    public final int rget(final int highestIndex, final T value) {
        return reverseMapSupplier.get().get(value);
    }

    public final int length() {
        return getChunk().size();
    }
}
