/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.chunkattributes;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Any;

/**
 * Attribute that specifies that a {@link Chunk} contains the bytes of objects which need to be decoded.
 */
public interface EncodedObjects extends Any {
}
