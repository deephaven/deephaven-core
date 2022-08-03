/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk.attributes;

import io.deephaven.chunk.Chunk;

/**
 * Attribute that specifies that a {@link Chunk} contains indices, e.g. positions or keys designating indices in a data
 * structure.
 */
public interface Indices extends Values {
}
