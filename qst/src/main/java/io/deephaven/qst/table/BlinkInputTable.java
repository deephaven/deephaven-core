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
 * Creates a blink input-table.
 */
@Immutable
@NodeStyle
public abstract class BlinkInputTable extends InputTableBase {

    public static BlinkInputTable of(TableSchema schema) {
        return ImmutableBlinkInputTable.builder()
                .schema(schema)
                .build();
    }

    public abstract TableSchema schema();

    @Default
    UUID id() {
        return UUID.randomUUID();
    }

    @Override
    public final <R> R walk(InputTable.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
