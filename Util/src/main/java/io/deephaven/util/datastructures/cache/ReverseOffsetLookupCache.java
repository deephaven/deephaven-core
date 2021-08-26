package io.deephaven.util.datastructures.cache;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * Caching data structure interface for caching Object to int (offset) mappings returned by an
 * expensive, idempotent lookup function from int to Object.
 */
public class ReverseOffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE>
    implements ToIntFunction<VALUE_TYPE> {

    private static final int NULL_INDEX = -1;

    private final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction;

    private final Map<VALUE_TYPE, CachedMapping<VALUE_TYPE>> reverseLookup =
        new KeyedObjectHashMap<>(getKeyDefinition());

    private volatile int highestKeyChecked = -1;

    public ReverseOffsetLookupCache(
        @NotNull final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction) {
        this.lookupFunction = Require.neqNull(lookupFunction, "lookupFunction");
    }

    /**
     * Ensure that the cache is populated for all {@code indexes <= highestIndexNeeded}.
     *
     * @param highestIndexNeeded The highest index needed for this operation
     */
    public void ensurePopulated(final int highestIndexNeeded,
        @NotNull final Supplier<EXTRA_INPUT_TYPE> extraFactory,
        @Nullable Consumer<EXTRA_INPUT_TYPE> extraCleanup) {
        if (highestIndexNeeded > highestKeyChecked) {
            synchronized (reverseLookup) { // Only let one thread through here at a time, to avoid
                                           // contention and redundant work.
                if (highestIndexNeeded > highestKeyChecked) {
                    final EXTRA_INPUT_TYPE extra = extraFactory.get();
                    try {
                        for (int key = highestKeyChecked + 1; key <= highestIndexNeeded; ++key) {
                            final VALUE_TYPE value = lookupFunction.lookup(key, extra);
                            if (value != null) {
                                reverseLookup.put(value, new CachedMapping<>(value, key));
                            }
                        }
                        highestKeyChecked = highestIndexNeeded;
                    } finally {
                        if (extraCleanup != null) {
                            extraCleanup.accept(extra);
                        }
                    }
                }
            }
        }
    }

    /**
     * Get the index of value in reverse lookup cache. Be sure to call
     * {@link #ensurePopulated(int, Supplier, Consumer)} for the appropriate index bound, first.
     *
     * @param value The value to look up
     * @return The index of value in the cache, or {@link #NULL_INDEX} (-1) if not found
     */
    @Override
    public int applyAsInt(final VALUE_TYPE value) {
        final CachedMapping<VALUE_TYPE> mapping = reverseLookup.get(value);
        return mapping == null ? NULL_INDEX : mapping.index;
    }

    /**
     * Release all resources held by the cache.
     */
    public void clear() {
        synchronized (reverseLookup) {
            highestKeyChecked = -1;
            reverseLookup.clear();
        }
    }

    /**
     * Implements a pair from a value in the range of the lookup function to the index at which it
     * occurs.
     *
     * @param <VALUE_TYPE>
     */
    private static class CachedMapping<VALUE_TYPE> {

        private final VALUE_TYPE value;
        private final int index;

        private CachedMapping(final VALUE_TYPE value, final int index) {
            this.value = value;
            this.index = index;
        }
    }

    /**
     * A key definition that uses value as the key.
     *
     * @param <VALUE_TYPE>
     */
    private static class CachedMappingKeyDef<VALUE_TYPE>
        extends KeyedObjectKey.Basic<VALUE_TYPE, CachedMapping<VALUE_TYPE>> {

        private static final KeyedObjectKey.Basic INSTANCE = new CachedMappingKeyDef();

        private CachedMappingKeyDef() {}

        @Override
        public final VALUE_TYPE getKey(CachedMapping<VALUE_TYPE> pair) {
            return pair.value;
        }
    }

    /**
     * Generic key definition instance accessor.
     */
    private static <VALUE_TYPE> KeyedObjectKey.Basic<VALUE_TYPE, CachedMapping<VALUE_TYPE>> getKeyDefinition() {
        // noinspection unchecked
        return CachedMappingKeyDef.INSTANCE;
    }
}
