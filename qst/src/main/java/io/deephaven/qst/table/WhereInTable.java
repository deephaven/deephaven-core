package io.deephaven.qst.table;

import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class WhereInTable extends TableBase {

    public abstract Table left();

    public abstract Table right();

    public abstract List<JoinMatch> matches();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
