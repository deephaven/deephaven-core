package io.deephaven.util.datastructures.cache;

/**
 * Lookup function interface for {@link OffsetLookupCache}s and {@link ReverseOffsetLookupCache}s to use.
 */
@FunctionalInterface
public interface OffsetLookup<VALUE_TYPE, EXTRA_INPUT_TYPE> {

    VALUE_TYPE lookup(int offset, EXTRA_INPUT_TYPE extra);
}
