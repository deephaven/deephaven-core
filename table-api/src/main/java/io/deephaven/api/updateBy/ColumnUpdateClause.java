package io.deephaven.api.updateBy;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.updateBy.spec.UpdateBySpec;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class ColumnUpdateClause implements UpdateByClause {
    public static ImmutableColumnUpdateClause.Builder builder() {
        return ImmutableColumnUpdateClause.builder();
    }

    public abstract UpdateBySpec spec();

    public abstract List<Pair> columns();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
