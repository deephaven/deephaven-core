package io.deephaven.qst.table;

import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import java.util.List;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class ExactJoinTable extends TableBase {

    public abstract Table left();

    public abstract Table right();

    public abstract List<JoinMatch> matches();

    public abstract List<JoinAddition> additions();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
