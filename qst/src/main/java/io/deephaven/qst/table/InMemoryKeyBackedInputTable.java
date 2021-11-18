package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.UUID;

/**
 * Creates an in-memory, append-only input table, with editable key rows.
 */
@Immutable
@NodeStyle
public abstract class InMemoryKeyBackedInputTable extends InputTableBase {

    public static InMemoryKeyBackedInputTable of(TableSchema schema, List<String> keys) {
        return ImmutableInMemoryKeyBackedInputTable.builder()
                .schema(schema)
                .addAllKeys(keys)
                .build();
    }

    public abstract TableSchema schema();

    /**
     * The keys that make up the "key" for the input table. May be empty.
     *
     * @return the keys
     */
    public abstract List<String> keys();

    @Default
    UUID id() {
        return UUID.randomUUID();
    }

    @Override
    public final <V extends InputTable.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
