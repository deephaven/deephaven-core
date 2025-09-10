//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

final class RowGroupTableIteratorVisitor implements RowGroupInfo.Visitor<Iterator<Table>> {

    public static Iterator<Table> of(final RowGroupInfo rowGroupInfo, final Table table) {
        return rowGroupInfo.walk(new RowGroupTableIteratorVisitor(table));
    }

    private static Iterator<Table> splitByMaxRows(final Table input, final long maxRows) {
        if (maxRows == Long.MAX_VALUE) {
            return List.of(input).iterator();
        }
        final long numRowGroups = (input.size() / maxRows) + ((input.size() % maxRows) > 0 ? 1 : 0);
        return splitByMaxGroups(input, numRowGroups);
    }

    private static Iterator<Table> splitByMaxGroups(final Table table, final long numGroups) {
        if (numGroups < 1) {
            throw new IllegalArgumentException("Number of groups must be at least 1, got: " + numGroups);
        } else if (numGroups == 1) {
            return List.of(table).iterator();
        }
        return new SplitEvenlyIterator(table, numGroups);
    }

    private final Table input;

    private RowGroupTableIteratorVisitor(final @NotNull Table input) {
        this.input = Objects.requireNonNull(input);
    }

    @Override
    public Iterator<Table> visit(final @NotNull RowGroupInfo.SingleGroup single) {
        return List.of(input).iterator();
    }

    @Override
    public Iterator<Table> visit(final @NotNull RowGroupInfo.MaxGroups maxGroups) {
        return splitByMaxGroups(input, maxGroups.numRowGroups());
    }

    @Override
    public Iterator<Table> visit(final @NotNull RowGroupInfo.MaxRows maxRows) {
        return splitByMaxRows(input, maxRows.maxRows());
    }

    @Override
    public Iterator<Table> visit(final @NotNull RowGroupInfo.ByGroups byGroups) {
        return new SplitByGroupsIterator(input, byGroups);
    }

    private static class SplitEvenlyIterator implements Iterator<Table> {
        private final Table input;
        private final long numRowGroups;

        private final long impliedRowGroupSz;
        private final long fractionalGroups;

        private long startOffset = 0;
        private long nextIter = 0;

        SplitEvenlyIterator(final @NotNull Table input, final long numRowGroups) {
            this.input = input;
            this.numRowGroups = Math.min(numRowGroups, input.size());

            this.impliedRowGroupSz = input.size() / numRowGroups;
            // number of groups which will have 1 additional row because rows are not evenly divisible by
            // numRowGroups
            this.fractionalGroups = input.size() % numRowGroups;
        }

        @Override
        public boolean hasNext() {
            return nextIter < numRowGroups;
        }

        @Override
        public Table next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            final RowSet rawRowSet = input.getRowSet();
            final long nextSz = impliedRowGroupSz + (nextIter++ < fractionalGroups ? 1 : 0);
            final WritableRowSet nextRows = rawRowSet.subSetByPositionRange(startOffset, startOffset += nextSz);
            return input.getSubTable(nextRows.toTracking());
        }
    }

    private static class SplitByGroupsIterator implements Iterator<Table> {
        private final Table[] partitionedTables;
        private final RowGroupInfo.ByGroups config;

        private int nextTable;
        private Iterator<Table> subIter;

        private SplitByGroupsIterator(final @NotNull Table input, final RowGroupInfo.ByGroups config) {
            final String[] groups = config.groups().toArray(String[]::new);
            ensureOrderedForGrouping(input, groups);
            this.partitionedTables = input.partitionBy(groups).constituents();
            this.config = config;
            this.nextTable = 0;
        }

        @Override
        public boolean hasNext() {
            return (nextTable < partitionedTables.length) || (subIter != null && subIter.hasNext());
        }

        @Override
        public Table next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            // if we're already working on a split sub-table, let the sub-table splitter handle this request
            if (subIter != null && subIter.hasNext()) {
                return subIter.next();
            }

            // else we've moved on to the next partitioned table
            final Table subTable = partitionedTables[nextTable++];
            subIter = splitByMaxRows(subTable, config.maxRows().orElse(Long.MAX_VALUE));
            return subIter.next();
        }
    }

    /**
     * Ensure that grouping will not change row-ordering
     *
     * @param input the table to be checked
     */
    private static void ensureOrderedForGrouping(final @NotNull Table input, final String[] groups) {
        final String origRowNum = "__OriginalRowNum__";
        final String newRowNum = "__PostGroupRowNum__";

        final Table misOrderedTbl = input
                .view(groups)
                .updateView(String.format("%s = ii", origRowNum))
                .groupBy(groups)
                .ungroup()
                .updateView(String.format("%s = ii", newRowNum))
                .where(String.format("%s != %s", origRowNum, newRowNum));

        if (!misOrderedTbl.isEmpty()) {
            throw new IllegalStateException(String.format("Misordered for Grouping column(s) %s:\n%s",
                    Arrays.toString(groups), TableTools.string(misOrderedTbl)));
        }
    }
}
