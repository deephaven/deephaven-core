//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.LeafStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * An empty table with {@link #size()} rows and {@code 0} columns.
 */
@Immutable
@LeafStyle
public abstract class EmptyTable extends TableBase {

    public static EmptyTable of(long size) {
        return ImmutableEmptyTable.of(size);
    }

    @Parameter
    public abstract long size();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkSize() {
        if (size() < 0) {
            throw new IllegalArgumentException("Must have non-negative size");
        }
    }
}
