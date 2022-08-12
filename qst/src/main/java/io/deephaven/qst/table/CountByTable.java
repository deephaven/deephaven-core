/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.ColumnName;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class CountByTable extends ByTableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableCountByTable.builder();
    }

    public abstract ColumnName countName();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends ByTableBase.Builder<CountByTable, CountByTable.Builder> {

        Builder countName(ColumnName countName);
    }
}
