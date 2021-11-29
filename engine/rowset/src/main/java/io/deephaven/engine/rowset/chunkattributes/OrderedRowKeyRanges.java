package io.deephaven.engine.rowset.chunkattributes;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;

/**
 * Attribute that specifies that a {@link Chunk} contains ordered row key ranges, which must be in strictly ascending
 * order.
 * <p>
 * These are to be represented as pairs of an inclusive start and an inclusive end in even and odd slots, respectively.
 */
public interface OrderedRowKeyRanges extends Values {
}
