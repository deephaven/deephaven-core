package io.deephaven.db.v2.locations.parquet.topage;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import io.deephaven.db.v2.sources.StringSetImpl;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import io.deephaven.util.datastructures.LazyCachingSupplier;
import org.apache.parquet.column.Dictionary;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Chunk-backed dictionary for use by {@link ToPage} implementations.
 */
public class ChunkDictionary<T, ATTR extends Attributes.Any>
        implements StringSetImpl.ReversibleLookup<T> {

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

    private final Lookup<T> lookup;
    private final Supplier<Dictionary> dictionarySupplier;

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
            @NotNull final Supplier<Dictionary> dictionarySupplier) {
        this.lookup = lookup;
        this.dictionarySupplier = dictionarySupplier;
        this.valuesSupplier = new LazyCachingSupplier<>(() -> {
            final Dictionary dictionary = dictionarySupplier.get();
            final T[] values = ObjectChunk.makeArray(dictionary.getMaxId() + 1);
            for (int ki = 0; ki < values.length; ++ki) {
                values[ki] = lookup.lookup(dictionary, ki);
            }
            return ObjectChunk.chunkWrap(values);
        });
        this.reverseMapSupplier = new LazyCachingSupplier<>(() -> {
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
