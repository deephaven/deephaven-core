//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Any;

/**
 * This is a marker interface for chunk sources that are agnostic of the row key when evaluating the value for a given
 * row key.
 */
public interface RowKeyAgnosticChunkSource<ATTR extends Any> extends FillUnordered<ATTR> {

}
