package io.deephaven.util.datastructures.cache;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

/**
 * Caching data structure for caching int (offset) to Object mappings. For use when lookup is
 * expensive but idempotent, and the range of offset indexes is relatively contiguous. This is only
 * suitable for lookup functions that return fully-initialized, immutable objects (or null).
 * <p>
 * This implementation stores data in a single contiguous array.
 */
public class ArrayBackedOffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE>
    extends BaseOffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE> {

    /**
     * The cached index to value mappings for this column source.
     */
    private volatile VALUE_TYPE[] cachedValues;

    /**
     * Construct a lookup cache for the supplied arguments, using
     * {@link OffsetLookupCache#createPlaceholder(Class)} to create a "null" placeholder value.
     *
     * @param valueType The value type
     * @param lookupFunction The lookup function from index to value, must return a
     *        fully-initialized, immutable object or null
     */
    public ArrayBackedOffsetLookupCache(@NotNull final Class<VALUE_TYPE> valueType,
        @NotNull final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction) {
        this(valueType, lookupFunction, OffsetLookupCache.createPlaceholder(valueType));
    }

    /**
     * Construct a lookup cache for the supplied arguments. instance.
     *
     * @param valueType The value type
     * @param lookupFunction The lookup function from index to value, must return a
     *        fully-initialized, immutable object or null
     * @param nullValue The "null" placeholder value, stored internally whenever lookupFunction
     *        returns null
     */
    private ArrayBackedOffsetLookupCache(@NotNull final Class<VALUE_TYPE> valueType,
        @NotNull final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction,
        @NotNull final VALUE_TYPE nullValue) {
        super(lookupFunction, nullValue, valueType);

        // noinspection unchecked
        this.cachedValues = (VALUE_TYPE[]) Array.newInstance(valueType, 0);
    }

    @Override
    public VALUE_TYPE get(final int index, final EXTRA_INPUT_TYPE extra) {
        Require.geqZero(index, "index");

        VALUE_TYPE[] localCachedValues = cachedValues;
        VALUE_TYPE value;

        if (index >= localCachedValues.length || (value = localCachedValues[index]) == null) {
            synchronized (this) {
                cachedValues =
                    localCachedValues = ArrayUtil.ensureSize(cachedValues, index + 1, valueType);
                if ((value = localCachedValues[index]) == null) {
                    value = lookupFunction.lookup(index, extra);
                    localCachedValues[index] = value == null ? nullValue : value;
                    return value;
                }
            }
        }

        return value == nullValue ? null : value;
    }

    @Override
    public synchronized void clear() {
        // noinspection unchecked
        cachedValues = (VALUE_TYPE[]) Array.newInstance(valueType, 0);
    }
}
