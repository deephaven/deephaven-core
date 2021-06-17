package io.deephaven.qst.table;

import io.deephaven.api.Filter;
import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class WhereTable extends TableBase implements SingleParentTable {

    public abstract Table parent();

    public abstract List<Filter> filters();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
