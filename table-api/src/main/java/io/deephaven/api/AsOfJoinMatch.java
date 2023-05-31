/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;


@Immutable
@SimpleStyle
public abstract class AsOfJoinMatch {

    public static AsOfJoinMatch of(
            final ColumnName leftColumn,
            final AsOfJoinRule joinRule,
            final ColumnName rightColumn) {
        return ImmutableAsOfJoinMatch.of(leftColumn, joinRule, rightColumn);
    }

    @Parameter
    public abstract ColumnName leftColumn();

    @Parameter
    public abstract AsOfJoinRule joinRule();

    @Parameter
    public abstract ColumnName rightColumn();

    public final String toRpcString() {
        return leftColumn().name() + joinRule().operatorString() + rightColumn().name();
    }
}
