package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.agg.key.Key;
import org.immutables.value.Value.Immutable;

@Immutable
@NodeStyle
public abstract class SingleAggregationTable extends ByTableBase {

    public static Builder builder() {
        return ImmutableSingleAggregationTable.builder();
    }

    public abstract Key key();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder extends ByTableBase.Builder<SingleAggregationTable, Builder> {
        Builder key(Key key);
    }
}
