/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.db.tables.select.SelectFilterFactory;
import io.deephaven.db.v2.select.ConjunctiveFilter;
import io.deephaven.db.v2.select.DisjunctiveFilter;
import io.deephaven.db.v2.select.SelectFilter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

public class WhereClause {

    static public Collection<SelectFilter> whereClause(SelectFilter... filters) {
        return Arrays.asList(filters);
    }

    static public Collection<SelectFilter> whereClause(String... strFilters) {
        return Arrays.asList(SelectFilterFactory.getExpressions(strFilters));
    }

    static public Collection<SelectFilter> whereClause() {
        return new ArrayList<>();
    }

    static public SelectFilter createDisjunctiveFilter(Collection<SelectFilter>[] filtersToApply) {
        ArrayList<SelectFilter> clauses = new ArrayList<>();

        for (Collection<SelectFilter> clause : filtersToApply) {
            clauses.add(ConjunctiveFilter
                .makeConjunctiveFilter(clause.toArray(new SelectFilter[clause.size()])));
        }

        return DisjunctiveFilter
            .makeDisjunctiveFilter(clauses.toArray(new SelectFilter[clauses.size()]));
    }
}
