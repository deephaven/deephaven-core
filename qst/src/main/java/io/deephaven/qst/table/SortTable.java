package io.deephaven.qst.table;

import io.deephaven.api.SortColumn;
import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class SortTable extends TableBase implements SingleParentTable {

    public abstract Table parent();

    public abstract List<SortColumn> columns();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
