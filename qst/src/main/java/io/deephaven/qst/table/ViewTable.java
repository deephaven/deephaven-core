package io.deephaven.qst.table;

import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class ViewTable extends TableBase {

    public abstract Table parent();

    public abstract List<String> columns();

    @Check
    final void checkNonEmpty() {
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
    }
}
