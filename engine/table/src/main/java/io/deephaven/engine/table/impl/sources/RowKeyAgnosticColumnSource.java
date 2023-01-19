/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.attributes.Any;

/**
 * This is a marker interface for column sources that are agnostic when fulfilling requested row keys.
 *
 * The marker extends from {@link InMemoryColumnSource} whether the column source is actually in memory or not; it would
 * be a waste to materialize the same value for all rows via select.
 */
public interface RowKeyAgnosticColumnSource<ATTR extends Any> extends FillUnordered<ATTR>, InMemoryColumnSource {

}
