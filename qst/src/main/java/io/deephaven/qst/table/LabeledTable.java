package io.deephaven.qst.table;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import javax.lang.model.SourceVersion;

/**
 * A labeled table is {@link #label() label} and {@link #table() table} pair.
 */
@Immutable
@SimpleStyle
public abstract class LabeledTable {

    public static LabeledTable of(String label, TableSpec table) {
        return ImmutableLabeledTable.of(label, table);
    }

    @Parameter
    public abstract String label();

    @Parameter
    public abstract TableSpec table();

    @Check
    final void checkNotEmpty() {
        if (label().isEmpty()) {
            throw new IllegalArgumentException(
                    "label is empty, must provide non-empty label for LabeledTable");
        }
    }
}
