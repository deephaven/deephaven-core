package io.deephaven.qst.table.agg;

import io.deephaven.qst.table.JoinMatch;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
public abstract class Distinct implements Aggregation {

    public static Distinct of(JoinMatch match) {
        return ImmutableDistinct.builder().match(match).build();
    }

    public static Distinct of(String x) {
        return of(JoinMatch.parse(x));
    }

    public abstract JoinMatch match();

    @Default
    public boolean includeNulls() {
        return false;
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
