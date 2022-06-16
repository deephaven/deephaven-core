package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.table.updateBySpec.UpdateBySpec;
import org.immutables.value.Value.Immutable;

import java.util.List;

@Immutable
@BuildableStyle
public abstract class ColumnUpdateClause implements UpdateByClause {
    public abstract UpdateBySpec spec();

    public abstract List<String> columns();

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
