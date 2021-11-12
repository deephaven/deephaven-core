package io.deephaven.engine.v2.sources;

import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for Object {@link ColumnSource}s that offer an alternative get method that takes a context.
 */
public interface ContextualizedObjectColumnSource<DATA_TYPE> extends ColumnSourceGetDefaults.ForObject<DATA_TYPE> {

    DATA_TYPE get(long index, @NotNull FillContext context);
}
