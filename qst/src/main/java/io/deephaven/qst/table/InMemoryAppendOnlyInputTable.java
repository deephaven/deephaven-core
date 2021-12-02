package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.UUID;

/**
 * Creates an in-memory, append-only input table.
 */
@Immutable
@NodeStyle
public abstract class InMemoryAppendOnlyInputTable extends InputTableBase {

    public static InMemoryAppendOnlyInputTable of(TableSchema schema) {
        return ImmutableInMemoryAppendOnlyInputTable.of(schema, UUID.randomUUID());
    }

    @Parameter
    public abstract TableSchema schema();

    @Parameter
    abstract UUID id();

    @Override
    public final <V extends InputTable.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
