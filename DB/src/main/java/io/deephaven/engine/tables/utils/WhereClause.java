/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables.utils;

import io.deephaven.engine.tables.select.SelectFilterFactory;
import io.deephaven.engine.v2.select.ConjunctiveFilter;
import io.deephaven.engine.v2.select.DisjunctiveFilter;
import io.deephaven.engine.v2.select.WhereFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class WhereClause {

    static public Collection<WhereFilter> whereClause(WhereFilter... filters) {
        return Arrays.asList(filters);
    }

    static public Collection<WhereFilter> whereClause(String... strFilters) {
        return Arrays.asList(SelectFilterFactory.getExpressions(strFilters));
    }

    static public Collection<WhereFilter> whereClause() {
        return new ArrayList<>();
    }

    static public WhereFilter createDisjunctiveFilter(Collection<WhereFilter>[] filtersToApply) {
        ArrayList<WhereFilter> clauses = new ArrayList<>();

        for (Collection<WhereFilter> clause : filtersToApply) {
            clauses.add(ConjunctiveFilter.makeConjunctiveFilter(clause.toArray(new WhereFilter[clause.size()])));
        }

        return DisjunctiveFilter.makeDisjunctiveFilter(clauses.toArray(new WhereFilter[clauses.size()]));
    }
}
