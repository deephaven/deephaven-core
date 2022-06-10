package io.deephaven.engine.table;

import io.deephaven.engine.table.updateBySpec.UpdateBySpec;
import org.jetbrains.annotations.NotNull;

public class ColumnUpdateClause implements UpdateByClause {
    private final UpdateBySpec spec;
    @NotNull
    private final String[] columns;

    ColumnUpdateClause(@NotNull final UpdateBySpec spec, @NotNull final String... columns) {
        this.spec = spec;
        this.columns = columns;
    }

    public UpdateBySpec getSpec() {
        return spec;
    }

    @NotNull
    public String[] getColumns() {
        return columns;
    }

    @Override
    public <V extends Visitor> V walk(@NotNull V v) {
        v.visit(this);
        return v;
    }
}
