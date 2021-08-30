package io.deephaven.util.datastructures.cache;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.SoftReference;
import java.lang.reflect.Array;

/**
 * Caching data structure for caching int (offset) to Object mappings. For use when lookup is expensive but idempotent,
 * and the range of offset indexes is relatively contiguous. This is only suitable for lookup functions that return
 * fully-initialized, immutable objects (or null).
 * <p>
 * This implementation stores data in an array of softly-reachable arrays, to enable unused regions to be reclaimed
 * under memory pressure.
 */
public class SoftArrayBackedOffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE>
        extends BaseOffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE> {

    private static final int LOG2_BLOCK_SIZE = 12;
    private static final int BLOCK_SIZE = 1 << LOG2_BLOCK_SIZE;
    private static final SoftReference[] ZERO_LENGTH_SOFT_REFERENCE_ARRAY = new SoftReference[0];

    private static int INDEX_TO_BLOCK_INDEX(final int index) {
        return index >> LOG2_BLOCK_SIZE;
    }

    private static int INDEX_TO_SUB_BLOCK_INDEX(final int index) {
        return index & (BLOCK_SIZE - 1);
    }

    /**
     * The cached index to value mappings for this column source.
     */
    private volatile SoftReference<VALUE_TYPE[]>[] cachedValues;

    /**
     * Construct a lookup cache for the supplied arguments, using {@link OffsetLookupCache#createPlaceholder(Class)} to
     * create a "null" placeholder value.
     *
     * @param valueType The value type
     * @param lookupFunction The lookup function from index to value, must return a fully-initialized, immutable object
     *        or null
     */
    public SoftArrayBackedOffsetLookupCache(@NotNull final Class<VALUE_TYPE> valueType,
            @NotNull final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction) {
        this(valueType, lookupFunction, OffsetLookupCache.createPlaceholder(valueType));
    }

    /**
     * Construct a lookup cache for the supplied arguments.
     *
     * @param valueType The value type
     * @param lookupFunction The lookup function from index to value, must return a fully-initialized, immutable object
     *        or null
     * @param nullValue The "null" placeholder value, stored internally whenever lookupFunction returns null
     */
    private SoftArrayBackedOffsetLookupCache(@NotNull final Class<VALUE_TYPE> valueType,
            @NotNull final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction,
            @NotNull final VALUE_TYPE nullValue) {
        super(lookupFunction, nullValue, valueType);

        // noinspection unchecked
        this.cachedValues = ZERO_LENGTH_SOFT_REFERENCE_ARRAY;
    }

    @Override
    public VALUE_TYPE get(final int index, final EXTRA_INPUT_TYPE extra) {
        Require.geqZero(index, "index");
        final int blockIndex = INDEX_TO_BLOCK_INDEX(index);
        final int subBlockIndex = INDEX_TO_SUB_BLOCK_INDEX(index);

        SoftReference<VALUE_TYPE[]>[] localCachedValues;
        SoftReference<VALUE_TYPE[]> blockRef;
        VALUE_TYPE[] block;
        VALUE_TYPE value;

        // This is only correct because we rely on lookupFunction to return fully-initialized, immutable objects (or
        // null).
        if ((localCachedValues = cachedValues).length <= blockIndex
                || (blockRef = localCachedValues[blockIndex]) == null
                || (block = blockRef.get()) == null
                || (value = block[subBlockIndex]) == null) {
            synchronized (this) {
                if ((localCachedValues = cachedValues).length <= blockIndex) {
                    // noinspection unchecked
                    final SoftReference<VALUE_TYPE[]>[] newCachedValues =
                            new SoftReference[1 << MathUtil.ceilLog2(blockIndex + 1)];
                    System.arraycopy(localCachedValues, 0, newCachedValues, 0, localCachedValues.length);
                    cachedValues = localCachedValues = newCachedValues;
                }
                if ((blockRef = localCachedValues[blockIndex]) == null
                        || (block = blockRef.get()) == null) {
                    // noinspection unchecked
                    block = (VALUE_TYPE[]) Array.newInstance(valueType, BLOCK_SIZE);
                    blockRef = new SoftReference<>(block);
                    localCachedValues[blockIndex] = blockRef;
                }
                if ((value = block[subBlockIndex]) == null) {
                    value = lookupFunction.lookup(index, extra);
                    block[subBlockIndex] = value == null ? nullValue : value;
                    return value;
                }
            }
        }

        return value == nullValue ? null : value;
    }

    @Override
    public synchronized void clear() {
        // noinspection unchecked
        cachedValues = ZERO_LENGTH_SOFT_REFERENCE_ARRAY;
    }
}
