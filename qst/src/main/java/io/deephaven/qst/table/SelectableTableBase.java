package io.deephaven.qst.table;

import org.immutables.value.Value.Check;

public abstract class SelectableTableBase extends TableBase implements SelectableTable {

    boolean skipNonEmptyCheck() {
        return false;
    }

    @Check
    final void checkNonEmpty() {
        if (skipNonEmptyCheck()) {
            return;
        }
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
    }

    @Check
    final void checkSelections() {
        if (columns().stream().distinct().count() != columns().size()) {
            throw new IllegalArgumentException(
                "Invalid selectable columns, must not use the same output column multiple times.");
        }
    }
}
