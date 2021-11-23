package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.datastructures.LongSizedDataStructure;

/**
 * Interface for {@link ColumnSource}s that know their size.
 */
public interface SizedColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE>, LongSizedDataStructure {
}
