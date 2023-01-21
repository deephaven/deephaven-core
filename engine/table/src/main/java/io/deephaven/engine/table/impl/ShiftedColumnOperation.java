/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.sources.ShiftedColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tools for creating a new ShiftedColumn(s) for a given input table and a source column(s)
 */
public class ShiftedColumnOperation {

    // static use only
    private ShiftedColumnOperation() {}

    /**
     * Creates a new table that has all the columns of the source table plus includes the new shifted column(s).
     *
     * @param source source table
     * @param shift the positive or negative shift value
     * @param matchColumns the source and shifted column pair(s) as shifted=source for example "X1=X", "Y1=Y"...
     * @return a new Table that includes the shifted column
     */
    public static Table addShiftedColumns(@NotNull Table source, long shift, @NotNull String... matchColumns) {
        String nuggetName = "addShiftedColumns ( " + shift + " , " + String.join(",", matchColumns) + ") ";
        return getShiftedColumnsUsingNugget(nuggetName, source, shift, MatchPairFactory.getExpressions(matchColumns));
    }

    /**
     * Creates a new table that has all the columns of the source table plus includes the new shifted column(s).
     *
     * @param source the source table, used to create new table with the shifted column
     * @param shift the constant shift value
     * @param matchPairs the source and shifted column pair(s) as shifted=source match pairs
     * @return a new Table that has all columns from source table plus additional shifted column(s) that are created
     *         using the shift from their source column(s)
     */
    public static Table addShiftedColumns(@NotNull Table source, long shift,
            @NotNull MatchPair... matchPairs) {
        String nuggetName = "addShiftedColumns ( shift , matchPairs )";
        return getShiftedColumnsUsingNugget(nuggetName, source, shift, matchPairs);
    }

    /**
     * Delegates to {@link ShiftedColumnOperation#getShiftedColumns(Table, long, MatchPair...)} using
     * QueryPerformanceRecorder.
     *
     * @param source the source table, used to create new table with the shifted column
     * @param shift the constant shift value
     * @param pairs the source and shifted column pair(s) as shifted=source match pairs
     * @return a new Table that has all columns from source table plus additional shifted column(s) that are created
     *         using the shift from their source column(s)
     */
    @NotNull
    private static Table getShiftedColumnsUsingNugget(
            @NotNull String nuggetName, @NotNull Table source, long shift, @NotNull MatchPair... pairs) {
        return QueryPerformanceRecorder.withNugget(nuggetName, source.sizeForInstrumentation(),
                () -> getShiftedColumns(source, shift, pairs));
    }

    /**
     * Encapsulates the logic to create and return a new Table that has all the columns from input table plus an
     * additional shifted column(s) that is based on the given source column(s) and the shift.
     *
     * @param source the source table, used to create new table with the shifted column
     * @param shift the constant shift value
     * @param pairs the source and shifted column pair(s) as shifted=source match pairs
     * @return a new Table that has all columns from input table plus additional shifted column(s) that are created
     *         using the shift from their source column(s)
     */
    @NotNull
    private static Table getShiftedColumns(@NotNull Table source, long shift, @NotNull MatchPair... pairs) {
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>(source.getColumnSourceMap());
        final Map<String, Set<String>> sourceToShiftModColSetMap = new LinkedHashMap<>();
        final Set<String> sourceColumns = new LinkedHashSet<>();
        final Set<String> shiftedColumns = new LinkedHashSet<>();

        Arrays.stream(pairs).forEach(pair -> {
            columnSourceMap.put(pair.leftColumn,
                    new ShiftedColumnSource<>(source.getRowSet(), source.getColumnSource(pair.rightColumn), shift));
            Set<String> set = sourceToShiftModColSetMap.computeIfAbsent(pair.rightColumn, s -> new LinkedHashSet<>());
            set.add(pair.rightColumn);
            set.add(pair.leftColumn);
            sourceColumns.add(pair.rightColumn);
            shiftedColumns.add(pair.leftColumn);
        });

        final QueryTable result = new QueryTable(source.getRowSet(), columnSourceMap);
        final Set<ModifiedColumnSet> resultTableMCSs = new LinkedHashSet<>();

        Arrays.stream(source.getDefinition().getColumnNamesArray()).forEach(colName -> {
            if (sourceToShiftModColSetMap.containsKey(colName)) {
                resultTableMCSs.add(
                        result.newModifiedColumnSet(sourceToShiftModColSetMap.get(colName).toArray(new String[0])));
            } else {
                resultTableMCSs.add(result.newModifiedColumnSet(colName));
            }
        });

        final QueryTable sourceAsQueryTable = (QueryTable) source;
        final ModifiedColumnSet sourceColumnSet =
                sourceAsQueryTable.newModifiedColumnSet(sourceColumns.toArray(new String[0]));
        final ModifiedColumnSet resultColumnSet = result.newModifiedColumnSet(shiftedColumns.toArray(new String[0]));
        final ModifiedColumnSet columnSetForDownstream = result.getModifiedColumnSetForUpdates();
        final ModifiedColumnSet.Transformer mcsTransformer = sourceAsQueryTable.newModifiedColumnSetTransformer(
                source.getDefinition().getColumnNamesArray(), resultTableMCSs.toArray(ModifiedColumnSet[]::new));

        if (source.isRefreshing()) {
            final BaseTable.ListenerImpl listener = new BaseTable.ListenerImpl("propagateUpdates", source, result) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream);
                    downstream.modifiedColumnSet = columnSetForDownstream;
                    mcsTransformer.clearAndTransform(
                            upstream.modifiedColumnSet(), downstream.modifiedColumnSet);

                    boolean updateColSetDownStream = upstream.modifiedColumnSet().containsAll(sourceColumnSet);

                    final RowSetBuilderRandom redirectedKeysBuilder = RowSetFactory.builderRandom();
                    final RowSetBuilderRandom previousRedirectedKeysBuilder = RowSetFactory.builderRandom();
                    final TrackingRowSet index = source.getRowSet();

                    try (final RowSet prevIndex = index.copyPrev()) {
                        if (upstream.removed().isNonempty()) {
                            updateColSetDownStream = true;
                            updateRedirectedKeys(prevIndex, upstream.removed(), shift, previousRedirectedKeysBuilder);
                        }

                        if (upstream.modified().isNonempty()
                                && upstream.modifiedColumnSet().containsAny(sourceColumnSet)) {
                            if (upstream.modifiedColumnSet().containsAll(sourceColumnSet)) {
                                updateColSetDownStream = true;
                            }
                            updateRedirectedKeys(index, upstream.modified(), shift, redirectedKeysBuilder);
                        }

                        if (upstream.added().isNonempty()) {
                            updateColSetDownStream = true;
                            updateRedirectedKeys(index, upstream.added(), shift, redirectedKeysBuilder);
                        }

                        if (updateColSetDownStream) {
                            columnSetForDownstream.setAll(resultColumnSet);
                        }
                    }

                    WritableRowSet modified = redirectedKeysBuilder.build();
                    downstream.modified = modified;
                    modified.insert(upstream.modified());
                    try (final WritableRowSet prevModified = previousRedirectedKeysBuilder.build()) {
                        prevModified.remove(upstream.removed());
                        upstream.shifted().apply(prevModified);
                        modified.insert(prevModified);
                    }
                    modified.remove(upstream.added());

                    result.notifyListeners(downstream);
                }
            };
            source.addUpdateListener(listener);
        }
        return result;
    }

    /**
     * The method invokes an inPlaceShift of 1 or -1 repeatedly until all redirected keys are collected in to the passed
     * in builder.
     *
     * @param rowSet Index to compute modified keys, prevIndex expected in case of removed and currentIndex otherwise
     * @param upstreamIndex the upstream rowSet (one of upstream.removed, upstream.modified or upstream.added depending
     *        on call)
     * @param shift the constant shift value
     * @param redirectedKeysBuilder the builder used to collect redirected keys
     */
    private static void updateRedirectedKeys(
            @NotNull RowSet rowSet,
            @NotNull RowSet upstreamIndex,
            long shift,
            @NotNull RowSetBuilderRandom redirectedKeysBuilder) {
        final WritableRowSet posRowSet = rowSet.invert(upstreamIndex);
        final long lastKeyPos = rowSet.size() - 1;
        for (long i = 0; i < Math.abs(shift); ++i) {
            if (shift < 0) {
                // the last position rowSet is removed to avoid any overflow errors.
                // In this case the shiftInPlace as of now handles the overflow without any errors
                // however, having the check eliminates this implicit dependency
                if (posRowSet.lastRowKey() == lastKeyPos) {
                    posRowSet.remove(lastKeyPos);
                }
                posRowSet.shiftInPlace(1);
            } else {
                // inPlaceShift of -1 is not allowed on key 0
                if (posRowSet.firstRowKey() == 0) {
                    posRowSet.remove(0);
                }
                posRowSet.shiftInPlace(-1);
            }
            if (posRowSet.isNonempty()) {
                try (final RowSet keysModified = rowSet.subSetForPositions(posRowSet)) {
                    redirectedKeysBuilder.addRowSet(keysModified);
                }
            }
        }
    }
}
