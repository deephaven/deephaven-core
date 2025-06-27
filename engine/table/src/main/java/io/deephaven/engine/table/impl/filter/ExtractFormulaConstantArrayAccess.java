//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.filter;

import io.deephaven.base.Pair;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.select.AbstractConditionFilter;
import io.deephaven.engine.table.impl.select.ConjunctiveFilter;
import io.deephaven.engine.table.impl.select.DisjunctiveFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterInvertedImpl;
import io.deephaven.engine.table.impl.select.WhereFilterRespectsBarrierImpl;
import io.deephaven.engine.table.impl.select.WhereFilterSerialImpl;

import java.util.List;
import java.util.Map;

public class ExtractFormulaConstantArrayAccess
        implements WhereFilter.Visitor<Pair<String, Map<Long, List<MatchPair>>>> {
    public static final ExtractFormulaConstantArrayAccess INSTANCE = new ExtractFormulaConstantArrayAccess();

    public static Pair<String, Map<Long, List<MatchPair>>> of(final WhereFilter filter) {
        return filter.walkWhereFilter(INSTANCE);
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final WhereFilter filter) {
        if (filter instanceof AbstractConditionFilter
                && ((AbstractConditionFilter) filter).hasConstantArrayAccess()) {
            return ((AbstractConditionFilter) filter).getFormulaShiftColPair();
        }
        return WhereFilter.Visitor.super.visitWhereFilter(filter);
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final WhereFilterInvertedImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final WhereFilterSerialImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final WhereFilterBarrierImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final WhereFilterRespectsBarrierImpl filter) {
        return visitWhereFilter(filter.getWrappedFilter());
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final DisjunctiveFilter filter) {
        // TODO NATE NOCOMMIT: we probably need to fetch all of the inner filters and combine them
        return null;
    }

    @Override
    public Pair<String, Map<Long, List<MatchPair>>> visitWhereFilter(final ConjunctiveFilter filter) {
        // TODO NATE NOCOMMIT: we probably need to fetch all of the inner filters and combine them
        return null;
    }
}
