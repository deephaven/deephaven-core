package io.deephaven.util.datastructures.cache;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

/**
 * Base class for offset-lookup cache implementations.
 */
abstract class BaseOffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE>
    implements OffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE> {

    /**
     * The type of the values in the cache.
     */
    final Class<VALUE_TYPE> valueType;

    /**
     * The lookup function that this cache is fronting.
     */
    final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction;

    /**
     * This VALUE_TYPE object represents a value that has been looked up and determined to be null,
     * as opposed to a value that has been reclaimed.
     */
    final VALUE_TYPE nullValue;

    BaseOffsetLookupCache(@NotNull final OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> lookupFunction,
        @NotNull final VALUE_TYPE nullValue,
        @NotNull final Class<VALUE_TYPE> valueType) {
        this.lookupFunction = Require.neqNull(lookupFunction, "lookupFunction");
        this.nullValue = Require.neqNull(nullValue, "nullValue");
        this.valueType = Require.neqNull(valueType, "valueType");
    }
}
