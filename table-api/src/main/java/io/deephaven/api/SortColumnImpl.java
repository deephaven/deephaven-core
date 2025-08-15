//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Represents a {@link #column() column} and {@link #order() order} pair.
 */
@Immutable
@SimpleStyle
public abstract class SortColumnImpl implements SortColumn {
    /**
     * The column name.
     *
     * @return the column name
     */
    @Parameter
    @Override
    public abstract ColumnName column();

    /**
     * The order.
     *
     * @return the order
     */
    @Parameter
    @Override
    public abstract Order order();
}
