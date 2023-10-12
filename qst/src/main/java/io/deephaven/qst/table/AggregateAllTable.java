/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.agg.spec.AggSpec;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class AggregateAllTable extends ByTableBase implements SingleParentTable {

    public static Builder builder() {
        return ImmutableAggregateAllTable.builder();
    }

    public abstract AggSpec spec();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }

    public interface Builder extends ByTableBase.Builder<AggregateAllTable, Builder> {
        Builder spec(AggSpec spec);
    }
}
