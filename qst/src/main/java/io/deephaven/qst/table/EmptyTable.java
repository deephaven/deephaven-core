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
    public final <V extends TableSpec.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkSize() {
        if (size() < 0) {
            throw new IllegalArgumentException("Must have non-negative size");
        }
    }
}
