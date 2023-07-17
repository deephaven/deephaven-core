/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
    public final <R> R walk(InputTable.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
