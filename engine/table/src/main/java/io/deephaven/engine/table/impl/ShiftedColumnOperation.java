//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.ShiftedColumnDefinition;
import io.deephaven.engine.table.impl.sources.ShiftedColumnSource;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
     * @param shifted the shifted column definition(s) that define the operation
     * @return a new Table that includes the shifted column
     */
    public static Table addShiftedColumns(
            final @NotNull Table source,
            final @NotNull ShiftedColumnDefinition... shifted) {
        final String shiftedName = Arrays.stream(shifted)
                .map(Object::toString)
                .collect(Collectors.joining(","));
        final String nuggetName = "addShiftedColumns ( " + shiftedName + ") ";
        return getShiftedColumnsUsingNugget(nuggetName, source,
                new LinkedHashSet<>(Arrays.stream(shifted).collect(Collectors.toList())));
    }

    /**
     * Creates a new table that has all the columns of the source table plus includes the new shifted column(s).
     *
     * @param source source table
     * @param shifted the shifted column definition(s) that define the operation
     * @return a new Table that includes the shifted column
     */
    public static Table addShiftedColumns(
            final @NotNull Table source,
            final @NotNull Set<ShiftedColumnDefinition> shifted) {
        final String shiftedName = shifted.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));
        final String nuggetName = "addShiftedColumns ( " + shiftedName + ") ";
        return getShiftedColumnsUsingNugget(nuggetName, source, shifted);
    }

    /**
     * Delegates to {@link ShiftedColumnOperation#getShiftedColumns(Table, Set)} using QueryPerformanceRecorder.
     *
     * @param source the source table, used to create new table with the shifted column
     * @param shifted the shifted column definition(s) that define the operation
     * @return a new Table that has all columns from source table plus additional shifted column(s) that are created
     *         using the shift from their source column(s)
     */
    @NotNull
    private static Table getShiftedColumnsUsingNugget(
            final @NotNull String nuggetName,
            final @NotNull Table source,
            final @NotNull Set<ShiftedColumnDefinition> shifted) {
        return QueryPerformanceRecorder.withNugget(nuggetName, source.sizeForInstrumentation(),
                () -> getShiftedColumns(source, shifted));
    }

    /**
     * Encapsulates the logic to create and return a new Table that has all the columns from input table plus any
     * additional shifted column(s).
     *
     * @param source the source table, used to create new table with the shifted column
     * @param shifted the shifted column definition(s) that define the operation
     * @return a new Table that has all columns from input table plus additional shifted column(s) created from the
     *         provided definition(s)
     */
    @NotNull
    private static Table getShiftedColumns(
            final @NotNull Table source,
            final @NotNull Set<ShiftedColumnDefinition> shifted) {

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>(source.getColumnSourceMap());
        final Map<String, Set<String>> sourceToShiftModColSetMap = new LinkedHashMap<>();
        final Set<String> sourceColumns = new LinkedHashSet<>();
        final Set<String> shiftedColumns = new LinkedHashSet<>();

        final long minShift = shifted.stream()
                .mapToLong(ShiftedColumnDefinition::getShiftAmount)
                .min()
                .orElse(0);
        final long maxShift = shifted.stream()
                .mapToLong(ShiftedColumnDefinition::getShiftAmount)
                .max()
                .orElse(0);

        shifted.forEach(column -> {
            columnSourceMap.put(column.getResultColumnName(), new ShiftedColumnSource<>(
                    source.getRowSet(), source.getColumnSource(column.getColumnName()), column.getShiftAmount()));
            final Set<String> columnsToDirty = sourceToShiftModColSetMap.computeIfAbsent(
                    column.getColumnName(), s -> new LinkedHashSet<>(List.of(column.getColumnName())));
            columnsToDirty.add(column.getColumnName());
            columnsToDirty.add(column.getResultColumnName());
            sourceColumns.add(column.getColumnName());
            shiftedColumns.add(column.getResultColumnName());
        });

        final QueryTable result = new QueryTable(source.getRowSet(), columnSourceMap);
        final Set<ModifiedColumnSet> resultTableMCSs = new LinkedHashSet<>();

        Arrays.stream(source.getDefinition().getColumnNamesArray()).forEach(colName -> {
            if (sourceToShiftModColSetMap.containsKey(colName)) {
                resultTableMCSs.add(result.newModifiedColumnSet(
                        sourceToShiftModColSetMap.get(colName).toArray(String[]::new)));
            } else {
                // add identity mapping for all other columns
                resultTableMCSs.add(result.newModifiedColumnSet(colName));
            }
        });

        final QueryTable sourceAsQueryTable = (QueryTable) source;
        final ModifiedColumnSet sourceColumnSet =
                sourceAsQueryTable.newModifiedColumnSet(sourceColumns.toArray(String[]::new));
        final ModifiedColumnSet dirtyColumnSet = result.newModifiedColumnSet(shiftedColumns.toArray(String[]::new));
        final ModifiedColumnSet downstreamColumnSet = result.getModifiedColumnSetForUpdates();
        final ModifiedColumnSet.Transformer mcsTransformer = sourceAsQueryTable.newModifiedColumnSetTransformer(
                source.getDefinition().getColumnNamesArray(), resultTableMCSs.toArray(ModifiedColumnSet[]::new));

        if (source.isRefreshing()) {
            final BaseTable.ListenerImpl listener = new BaseTable.ListenerImpl("propagateUpdates", source, result) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    final TableUpdateImpl downstream = TableUpdateImpl.copy(upstream, downstreamColumnSet);
                    mcsTransformer.clearAndTransform(
                            upstream.modifiedColumnSet(), downstream.modifiedColumnSet);

                    WritableRowSet modifiedRows = null;
                    boolean dirtyModifiedColumnSet = false;

                    if (upstream.removed().isNonempty()) {
                        try (final RowSet prevRowSet = source.getRowSet().copyPrev();
                                final WritableRowSet dirtyRowSet = computeDirtyModifiedRowSetInPositionSpace(
                                        prevRowSet, upstream.removed(), minShift, maxShift)) {
                            if (dirtyRowSet.isNonempty()) {
                                modifiedRows = prevRowSet.subSetForPositions(dirtyRowSet);
                                dirtyModifiedColumnSet = true;

                                // move the dirty rows into current keyspace
                                if (upstream.shifted().nonempty()) {
                                    upstream.shifted().apply(modifiedRows);
                                }
                            }
                        }
                    }

                    final boolean haveAdds = upstream.added().isNonempty();
                    final boolean haveMods = upstream.modified().isNonempty()
                            && upstream.modifiedColumnSet().containsAny(sourceColumnSet);

                    // we'll combine adds and mods to coalesce ranges prior to subSetForPositions
                    if (haveAdds || haveMods) {
                        try (final WritableRowSet dirtyFromAdds = haveAdds ? computeDirtyModifiedRowSetInPositionSpace(
                                source.getRowSet(), upstream.added(), minShift, maxShift) : null;
                                final WritableRowSet dirtyFromMods =
                                        haveMods ? computeDirtyModifiedRowSetInPositionSpace(
                                                source.getRowSet(), upstream.modified(), minShift, maxShift) : null) {

                            if (haveAdds && dirtyFromAdds.isNonempty()) {
                                dirtyModifiedColumnSet = true;
                            }
                            // note dirtyFromMods is propagated by the mcsTransformer

                            final RowSet dirtyPositions = haveAdds ? dirtyFromAdds : dirtyFromMods;
                            if (haveAdds && haveMods) {
                                dirtyFromAdds.insert(dirtyFromMods);
                            }

                            if (dirtyPositions.isNonempty()) {
                                final WritableRowSet dirtyRowSet =
                                        source.getRowSet().subSetForPositions(dirtyPositions);

                                if (haveAdds && haveMods) {
                                    // modifications might dirty added rows
                                    dirtyRowSet.remove(upstream.added());
                                }

                                if (dirtyRowSet.isEmpty()) {
                                    dirtyRowSet.close();
                                } else if (modifiedRows == null) {
                                    modifiedRows = dirtyRowSet;
                                } else {
                                    modifiedRows.insert(dirtyRowSet);
                                    dirtyRowSet.close();
                                }
                            }
                        }
                    }

                    if (dirtyModifiedColumnSet) {
                        downstreamColumnSet.setAll(dirtyColumnSet);
                    }

                    if (modifiedRows != null) {
                        // close the copied upstream set
                        downstream.modified.close();
                        modifiedRows.insert(upstream.modified());
                        downstream.modified = modifiedRows;
                    } // else we can use the existing upstream modified row set

                    result.notifyListeners(downstream);
                }
            };
            source.addUpdateListener(listener);
        }
        return result;
    }

    /**
     * Computes the modified rowSet based on the sourceRowSet and the updatedRowSet. The provided rowSets must be in the
     * same keyspace (previous, or current). The resulting rowSet is in position space and will not include any rows
     * that are in the updatedRowSet.
     *
     * @param sourceRowSet the source's row set
     * @param updatedRowSet the updated row set (removed, updated, or modified)
     * @param minShift the maximum negative shift value
     * @param maxShift the maximum positive shift value
     * @return the effective updatedRowSet in position space of the sourceRowSet after applying both shifts
     */
    private static WritableRowSet computeDirtyModifiedRowSetInPositionSpace(
            final @NotNull RowSet sourceRowSet,
            final @NotNull RowSet updatedRowSet,
            final long minShift,
            final long maxShift) {
        if (minShift == 0 && maxShift == 0) {
            return RowSetFactory.empty();
        }
        if (minShift == maxShift || maxShift == 0) {
            return computeDirtyModifiedRowSetInPositionSpace(sourceRowSet, updatedRowSet, minShift);
        }
        if (minShift == 0) {
            return computeDirtyModifiedRowSetInPositionSpace(sourceRowSet, updatedRowSet, maxShift);
        }

        WritableRowSet result = computeDirtyModifiedRowSetInPositionSpace(sourceRowSet, updatedRowSet, minShift);
        result = computeDirtyModifiedRowSetInPositionSpace(sourceRowSet, result, maxShift);
        return result;
    }

    /**
     * Computes the modified rowSet based on the sourceRowSet and the updatedRowSet. The provided rowSets must be in the
     * same keyspace (previous, or current). The resulting rowSet is in position space and will not include any rows
     * that are in the updatedRowSet.
     *
     * @param sourceRowSet the source's row set
     * @param updatedRowSet the updated row set (removed, updated, or modified)
     * @param shift the shift value
     * @return the effective updatedRowSet in position space of the sourceRowSet after applying the shift
     */
    private static WritableRowSet computeDirtyModifiedRowSetInPositionSpace(
            @NotNull RowSet sourceRowSet, @NotNull RowSet updatedRowSet, long shift) {
        final MutableLong minKeyAllowed = new MutableLong(0);
        final RowSetBuilderSequential rowSetBuilder = RowSetFactory.builderSequential();

        // when shift < 0, we need to look-ahead to avoid including rows that are part of the updatedRowSet
        final MutableLong prevRangeEnd = new MutableLong(-1);
        try (final WritableRowSet posRowSet = sourceRowSet.invert(updatedRowSet)) {
            posRowSet.forAllRowKeyRanges((s, e) -> {
                final long dirtyStart;
                final long dirtyEnd;

                if (shift > 0) {
                    dirtyStart = Math.max(minKeyAllowed.get(), s - shift);
                    dirtyEnd = s - 1;
                    minKeyAllowed.set(e + 1);
                } else {
                    final long pEnd = prevRangeEnd.get();
                    prevRangeEnd.set(e);
                    if (pEnd == -1) {
                        return;
                    }

                    dirtyStart = pEnd + 1;
                    dirtyEnd = Math.min(s - 1, pEnd - shift);
                }

                if (dirtyStart <= dirtyEnd) {
                    rowSetBuilder.appendRange(dirtyStart, dirtyEnd);
                }
            });
        }

        // if we were looking ahead, we need to process the last range
        final long pEnd = prevRangeEnd.get();
        if (shift < 0 && pEnd != -1 && pEnd + 1 < sourceRowSet.size()) {
            final long dirtyStart = pEnd + 1;
            final long dirtyEnd = pEnd - shift;
            rowSetBuilder.appendRange(dirtyStart, dirtyEnd);
        }

        return rowSetBuilder.build();
    }
}
