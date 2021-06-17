package io.deephaven.qst.table;

import io.deephaven.api.Selectable;
import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SelectTable extends TableBase implements SingleParentTable {

    public abstract Table parent();

    public abstract List<Selectable> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public final boolean isSelectAll() {
        return columns().isEmpty();
    }
}
