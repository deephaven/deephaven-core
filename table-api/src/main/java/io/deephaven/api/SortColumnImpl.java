//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class SortColumnImpl implements SortColumn {
    @Parameter
    @Override
    public abstract ColumnName column();

    @Parameter
    @Override
    public abstract Order order();
}
