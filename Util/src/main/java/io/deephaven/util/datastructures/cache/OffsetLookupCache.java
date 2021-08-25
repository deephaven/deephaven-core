package io.deephaven.util.datastructures.cache;

import org.jetbrains.annotations.NotNull;

/**
 * Caching data structure interface for caching int (offset) to Object mappings.
 */
public interface OffsetLookupCache<VALUE_TYPE, EXTRA_INPUT_TYPE> {

    /**
     * Lookup the value for index.
     *
     * @param index The offset (must be &gt;= 0)
     * @return The result of calling the lookup function for index, possibly from cache
     */
    VALUE_TYPE get(int index, EXTRA_INPUT_TYPE extra);

    /**
     * Release all resources held by the cache.
     */
    void clear();

    /**
     * Attempt to instantiate an instance of valueType with the nullary constructor, in order to
     * create a placeholder instance reference.
     *
     * @param valueType The type of the desired placeholder
     * @return A placeholder instance reference
     */
    static <VALUE_TYPE> VALUE_TYPE createPlaceholder(@NotNull final Class<VALUE_TYPE> valueType) {
        try {
            return valueType.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnsupportedOperationException("Could not instantiate " + valueType, e);
        }
    }
}
