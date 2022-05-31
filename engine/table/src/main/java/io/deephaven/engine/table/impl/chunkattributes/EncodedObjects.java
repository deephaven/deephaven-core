package io.deephaven.engine.table.impl.chunkattributes;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Attribute that specifies that a {@link Chunk} contains the bytes of objects which need to be decoded.
 */
public interface EncodedObjects extends Any {
}
