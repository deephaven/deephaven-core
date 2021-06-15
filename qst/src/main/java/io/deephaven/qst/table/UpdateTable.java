package io.deephaven.qst.table;

import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class UpdateTable extends TableBase implements SingleParentTable {

    public abstract Table parent();

    public abstract List<Selectable> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkNonEmpty() {
        if (columns().isEmpty()) {
            throw new IllegalArgumentException("columns must not be empty");
        }
    }
}
