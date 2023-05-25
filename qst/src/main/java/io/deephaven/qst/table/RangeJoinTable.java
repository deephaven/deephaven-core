/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.qst.table;

import io.deephaven.annotations.NodeStyle;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RangeJoinMatch;
import io.deephaven.api.agg.Aggregation;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.List;

/**
 * @see io.deephaven.api.TableOperations#rangeJoin(Object, Collection, RangeJoinMatch, Collection)
 */
@Immutable
@NodeStyle
public abstract class RangeJoinTable extends TableBase {

    public static Builder builder() {
        return ImmutableRangeJoinTable.builder();
    }

    public abstract TableSpec left();

    public abstract TableSpec right();

    public abstract List<JoinMatch> exactMatches();

    public abstract RangeJoinMatch rangeMatch();

    public abstract List<Aggregation> aggregations();

    @Check
    final void checkAggregationsNonEmpty() {
        if (aggregations().isEmpty()) {
            throw new IllegalArgumentException("Aggregations must not be empty");
        }
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    public interface Builder {

        Builder left(TableSpec left);

        Builder right(TableSpec right);

        Builder addExactMatches(JoinMatch exactMatch);

        Builder addExactMatches(JoinMatch... exactMatches);

        Builder addAllExactMatches(Iterable<? extends JoinMatch> exactMatches);

        Builder rangeMatch(RangeJoinMatch rangeMatch);

        Builder addAggregations(Aggregation aggregation);

        Builder addAggregations(Aggregation... aggregations);

        Builder addAllAggregations(Iterable<? extends Aggregation> aggregations);

        RangeJoinTable build();
    }
}
