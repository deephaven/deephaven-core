//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.UUID;

/**
 * Creates a blink input-table.
 */
@Immutable
@NodeStyle
public abstract class BlinkInputTable extends InputTableBase {

    public static BlinkInputTable of(TableSchema schema) {
        return ImmutableBlinkInputTable.of(schema, UUID.randomUUID());
    }

    @Parameter
    public abstract TableSchema schema();

    @Parameter
    abstract UUID id();

    @Override
    public final <R> R walk(InputTable.Visitor<R> visitor) {
        return visitor.visit(this);
    }
}
