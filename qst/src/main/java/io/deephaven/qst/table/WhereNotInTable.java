package io.deephaven.qst.table;

import io.deephaven.api.JoinMatch;
import java.util.List;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

@Immutable
public abstract class WhereNotInTable extends TableBase {

    public abstract Table left();

    public abstract Table right();

    public abstract List<JoinMatch> matches();

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Check
    final void checkNotEmpty() {
        if (matches().isEmpty()) {
            throw new IllegalArgumentException("Must not be empty");
        }
    }
}
