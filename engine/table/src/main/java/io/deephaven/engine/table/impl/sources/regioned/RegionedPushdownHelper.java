//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.util.JobScheduler;

import java.util.*;
import java.util.stream.Stream;

public class RegionedPushdownHelper {
    /**
     * The context for regioned pushdown parallel JobScheduler operations.
     */
    public static class RegionThreadContext implements JobScheduler.JobThreadContext {
        RowSet shiftedRowSet;

        @Override
        public void close() {
            try (RowSet ignored = shiftedRowSet) {
            }
        }

        public void reset() {
            if (shiftedRowSet != null) {
                shiftedRowSet.close();
                shiftedRowSet = null;
            }
        }
    }

    /**
     * Combine the results from multiple regioned pushdown operations into a unified result.
     */
    public static PushdownResult buildResults(
            final WritableRowSet[] matches,
            final WritableRowSet[] maybeMatches,
            final RowSet selection) {
        final long totalMatchSize = Stream.of(matches).mapToLong(RowSet::size).sum();
        final long totalMaybeMatchSize = Stream.of(maybeMatches).mapToLong(RowSet::size).sum();
        final long selectionSize = selection.size();
        if (totalMatchSize == selectionSize) {
            Assert.eqZero(totalMaybeMatchSize, "totalMaybeMatchSize");
            return PushdownResult.allMatch(selection);
        }
        if (totalMaybeMatchSize == selectionSize) {
            Assert.eqZero(totalMatchSize, "totalMatchSize");
            return PushdownResult.allMaybeMatch(selection);
        }
        if (totalMatchSize == 0 && totalMaybeMatchSize == 0) {
            return PushdownResult.allNoMatch(selection);
        }
        // Note: it's not obvious what the best approach for building these RowSets is; that is, sequential
        // insertion vs sequential builder. We know that the individual results are ordered and non-overlapping.
        // If this becomes important, we can do more benchmarking.
        try (
                final WritableRowSet match = RowSetFactory.unionInsert(Arrays.asList(matches));
                final WritableRowSet maybeMatch = RowSetFactory.unionInsert(Arrays.asList(maybeMatches))) {
            return PushdownResult.of(selection, match, maybeMatch);
        }
    }
}
