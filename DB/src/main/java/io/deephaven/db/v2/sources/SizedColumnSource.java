package io.deephaven.db.v2.sources;

import io.deephaven.db.util.LongSizedDataStructure;

/**
 * Interface for {@link ColumnSource}s that know their size.
 */
public interface SizedColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE>, LongSizedDataStructure {
}
