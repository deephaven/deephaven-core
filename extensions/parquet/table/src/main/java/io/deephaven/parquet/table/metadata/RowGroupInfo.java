//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public abstract class RowGroupInfo {
    private static final RowGroupInfo DEFAULT = new SingleRowGroup();

    /**
     * The default RowGroupInfo implementation. All data is within a single RowGroup
     *
     * @return a {@link RowGroupInfo} which uses only a single RowGroup
     */
    @ScriptApi
    public static RowGroupInfo defaultRowGroupInfo() {
        return DEFAULT;
    }

    /**
     * Splits into a number of RowGroups, each of which has no more than {@code maxRows} rows
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @return a {@link RowGroupInfo} which splits the input into a number of RowGroups, each containing no more than
     *         {@code maxRows} rows
     */
    @ScriptApi
    public static RowGroupInfo withMaxRows(final long maxRows) {
        return new SplitByMaxRows(maxRows);
    }

    /**
     * Split evenly into a pre-defined number of RowGroups
     *
     * @param numRowGroups the number of RowGroups to write
     * @return A {@link RowGroupInfo} which splits the input into a pre-defined number of RowGroups
     */
    @ScriptApi
    public static RowGroupInfo splitEvenly(final int numRowGroups) {
        return numRowGroups <= 1 ? defaultRowGroupInfo() : new SplitEvenly(numRowGroups);
    }

    /**
     * Splits each unique group into a RowGroup. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the `writeTable(...)` call
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    public static RowGroupInfo byGroup(final String... groups) {
        return byGroup(Long.MAX_VALUE, groups);
    }

    /**
     * Splits each unique group into a number of RowGroups. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the `writeTable(...)` call
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a number of RowGroups per unique grouping-value
     */
    @ScriptApi
    public static RowGroupInfo byGroup(final long maxRows, final String... groups) {
        return new SplitByGroups(maxRows, groups);
    }

    /**
     * Allows for custom breakdown of RowGroups. If the ordering of the `.merge(...)` of the Tables does not match the
     * input-table, then an exception will be thrown during the `writeTable(...)` call
     *
     * @param func a function that splits the input `Table` into a `Table[]` without changing row-ordering
     * @return a {@link RowGroupInfo} which will perform a custom split of groups
     */
    @ScriptApi
    public static RowGroupInfo customFunction(final Function<Table, Table[]> func) {
        return new CustomSplit(func);
    }

    /**
     * Splits the input {@link Table} into an array of sub-Tables, each of which represents a single RowGroup to be
     * written
     *
     * @param input the {@link Table} which may be split
     * @return an array (possibly of 1) of sub-Tables
     */
    public abstract Table[] splitForRowGroups(@NotNull final Table input);

    /**
     * Splits a {@link Table} evenly into a defined number of sub-Tables
     *
     * @param input the {@link Table} to be split
     * @param numSubTables the desired number of sub-Tables
     * @return an Array of {@code numSubTables} length of {@link Table} instances
     */
    public static Table[] splitEvenly(@NotNull final Table input, final int numSubTables) {
        if (numSubTables <= 1) {
            return new Table[] {input};
        }

        final long impliedRowGroupSz = input.size() / numSubTables;
        // number of groups which will have 1 additional row because rows are not evenly divisible by numSubTables
        final long fractionalGroups = input.size() % numSubTables;

        final Table[] ret = new Table[numSubTables];
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final RowSet rawRowSet = input.getRowSet();

            long startOffset = 0;
            for (int ii = 0; ii < numSubTables; ii++) {
                final long nextSz = impliedRowGroupSz + (ii < fractionalGroups ? 1 : 0);
                final WritableRowSet nextRows = rawRowSet.subSetByPositionRange(startOffset, startOffset += nextSz);
                ret[ii] = input.getSubTable(nextRows.toTracking());
            }

            try (final RowSet leftoverRows = rawRowSet.subSetByPositionRange(startOffset, Long.MAX_VALUE)) {
                if (!leftoverRows.isEmpty()) {
                    throw new IllegalStateException("Has more rows: " + leftoverRows);
                }
            }
        }

        return ret;
    }

    // @formatter:off
    /*
    public static Table[] splitEvenly(@NotNull final Table input, final int numSubTables) {
        if (numSubTables <= 1) {
            return new Table[] {input};
        }

        final long suggestedRowGroupSize = input.size() / numSubTables;
        // number of groups which will have 1 additional row because rows are not evenly divisible by numSubTables
        final long fractionalGroups = input.size() % numSubTables;

        final Table[] ret = new Table[numSubTables];

        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final RowSequence.Iterator it = input.getRowSet().getRowSequenceIterator();
            for (int ii = 0; ii < numSubTables; ii++) {
                if (!it.hasMore()) {
                    throw new IllegalStateException("No more rows!");
                }

                final long nextSz = suggestedRowGroupSize + (ii < fractionalGroups ? 1 : 0);
                final RowSequence next = it.getNextRowSequenceWithLength(nextSz);
                try (final RowSet nextRowSet = next.asRowSet()) {
                    ret[ii] = input.getSubTable(nextRowSet.copy().toTracking());
                }
            }

            if (it.hasMore()) {
                try (final RowSet leftoverRows = it.getNextRowSequenceWithLength(Long.MAX_VALUE).asRowSet()) {
                    throw new IllegalStateException(String.format("Subtable dropped rows; Iterator has more: %s",
                            leftoverRows));
                }
            }
        }

        return ret;
    }
     */
    // @formatter:on

    /**
     * Keeps all rows within a single RowGroup
     */
    private static class SingleRowGroup extends RowGroupInfo {
        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            return new Table[] {input};
        }
    }

    /**
     * Splits evenly across a number of RowGroups, ensuring that no group is larger than {@code maxRows}
     */
    private static class SplitByMaxRows extends RowGroupInfo {
        private final long maxRows;

        private SplitByMaxRows(long maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("MaxRows must be positive");
            }
            this.maxRows = maxRows;
        }

        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            final long numRowGroups = (input.size() / (maxRows - 1)) + 1;
            if (numRowGroups > Integer.MAX_VALUE) {
                throw new IllegalStateException("Number of RowGroups is greater than Integer.MAX_VALUE");
            }
            return splitEvenly(input, (int) numRowGroups);
        }
    }

    /**
     * Splits evenly across {@code numRowGroups} RowGroups
     */
    private static class SplitEvenly extends RowGroupInfo {
        private final int numRowGroups;

        private SplitEvenly(int numRowGroups) {
            this.numRowGroups = numRowGroups;
        }

        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            return splitEvenly(input, numRowGroups);
        }
    }

    /**
     * Splits into RowGroups based on unique values within {@code groups}, and optionally further splits (per unique
     * {@code groups} ensuring that no group is larger than {@code maxRows}. If {@code maxRows} is not desired, this
     * parameter may be set to {@code Long.MAX_VALUE}
     */
    private static class SplitByGroups extends CustomSplit {
        private SplitByGroups(final long maxRows, final String[] groups) {
            super(table -> splitByGroups(table, maxRows, groups));
            if (groups == null || groups.length == 0) {
                throw new IllegalArgumentException("Requires at least one group");
            }
        }

        private static Table[] splitByGroups(@NotNull final Table input, final long maxRows, final String[] groups) {
            final Table[] partitionedTables = input.partitionBy(groups).constituents();

            // no need to split further if `maxRows` is greater than our original Table (which includes Long.MAX_VALUE)
            if (maxRows >= input.size()) {
                return partitionedTables;
            }

            final RowGroupInfo maxRowsInfo = new SplitByMaxRows(maxRows);

            int rowGroupCnt = 0;
            final Table[][] maxSplit = new Table[partitionedTables.length][];
            for (int ii = 0; ii < partitionedTables.length; ii++) {
                maxSplit[ii] = maxRowsInfo.splitForRowGroups(partitionedTables[ii]);
                rowGroupCnt += maxSplit[ii].length;
            }

            if (rowGroupCnt == partitionedTables.length) {
                // all RowGroups already fit within `maxRows`
                return partitionedTables;
            }

            // else we need to reconstitute the `Table[][]` into a straight `Table[]` of proper length
            final Table[] retTables = new Table[rowGroupCnt];
            int nextTarget = 0;
            for (final Table[] tables : maxSplit) {
                System.arraycopy(tables, 0, retTables, nextTarget, tables.length);
                nextTarget += tables.length;
            }

            return retTables;
        }
    }

    /**
     * Splits into RowGroups based on a passed in {@code Function<Table, Table[]>}. Ordering of the {@link RowSet}s of
     * the sub-tables must not be changed from the ordering of the input Table or an Exception will be thrown
     */
    private static class CustomSplit extends RowGroupInfo {
        final Function<Table, Table[]> customFunction;

        private CustomSplit(final Function<Table, Table[]> customFunction) {
            if (customFunction == null) {
                throw new IllegalArgumentException("null Function not permitted");
            }
            this.customFunction = customFunction;
        }

        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            final Table[] splitTables;
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                splitTables = customFunction.apply(input);
                ensureConsistentOrdering(input, splitTables);
            }
            return splitTables;
        }
    }

    /**
     * Ensure that table-ordering has not changed
     *
     * @param origTbl the original (pre-split) table
     * @param ordered the array-split table
     */
    private static void ensureConsistentOrdering(@NotNull final Table origTbl, @NotNull final Table[] ordered) {
        final RowSequence.Iterator it = origTbl.getRowSet().getRowSequenceIterator();
        for (final Table subTable : ordered) {
            final RowSet newRows = subTable.getRowSet();
            final long subSize = newRows.size();

            try (final RowSet origRows = it.getNextRowSequenceWithLength(subSize).asRowSet()) {
                if (!newRows.equals(origRows)) {
                    throw new IllegalStateException(String
                            .format("Subtable ordering mismatch;\n  Expected: %s\n  Received: %s", origRows, newRows));
                }
            }
        }

        if (it.hasMore()) {
            try (final RowSet leftoverRows = it.getNextRowSequenceWithLength(Long.MAX_VALUE).asRowSet()) {
                throw new IllegalStateException(String.format("Ordered Tables dropped rows; Iterator has more: %s",
                        leftoverRows));
            }
        }
    }
}
