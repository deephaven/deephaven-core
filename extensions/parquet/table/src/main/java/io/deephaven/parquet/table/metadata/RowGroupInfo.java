//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.Table;
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
     * Splits each unique group into a RowGroup. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the `writeTable(...)` call
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    public static RowGroupInfo byGroup(final String... groups) {
        return new SplitByGroups(groups);
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

    private static class SingleRowGroup extends RowGroupInfo {
        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            return new Table[] {input};
        }
    }

    private static class SplitEvenly extends RowGroupInfo {
        private final int numRowGroups;

        private SplitEvenly(int numRowGroups) {
            this.numRowGroups = numRowGroups;
        }

        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            final long suggestedRowGroupSize = input.size() / numRowGroups;
            // number of groups which will have 1 additional row because rows are not evenly divisible by numRowGroups
            final long fractionalGroups = input.size() % numRowGroups;

            final Table[] ret = new Table[numRowGroups];

            long startOffset = 0;
            for (int ii = 0; ii < numRowGroups; ii++) {
                final long subSize = suggestedRowGroupSize + (ii < fractionalGroups ? 1 : 0);
                ret[ii] = subTable(input, startOffset, subSize);
                startOffset += subSize;
            }

            return ret;
        }
    }

    private static class SplitByMaxRows extends RowGroupInfo {
        private final long maxRows;

        private SplitByMaxRows(long maxRows) {
            this.maxRows = maxRows;
        }

        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            final int numRowGroups = (int) (input.size() / maxRows) + 1;
            if (numRowGroups == 1) {
                return new Table[] {input};
            }
            final Table[] ret = new Table[numRowGroups];

            long startOffset = 0;
            for (int ii = 0; ii < numRowGroups; ii++) {
                final long subSize = maxRows;
                ret[ii] = subTable(input, startOffset, subSize);
                startOffset += subSize;
            }

            return ret;
        }
    }

    private static class SplitByGroups extends CustomSplit {
        private SplitByGroups(final String[] groups) {
            super(table -> table.partitionBy(groups).constituents());
            if (groups == null || groups.length == 0) {
                throw new IllegalArgumentException("Requires at least one group");
            }
        }
    }

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
            final Table[] splitTables = customFunction.apply(input);
            RowGroupInfo.ensureConsistentOrdering(input, splitTables);
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
        long startOffset = 0;
        for (final Table rowSet : ordered) {
            final RowSet orderedRows = rowSet.getRowSet();
            final long subSize = orderedRows.size();

            final RowSet origRows = subTable(origTbl, startOffset, subSize).getRowSet();

            if (orderedRows.intersect(origRows).size() != subSize) {
                throw new IllegalStateException(String
                        .format("Subtable ordering mismatch;\n  Expected: %s\n  Received: %s", origRows, orderedRows));
            }

            startOffset += subSize;
        }

        if (origTbl.size() != startOffset) {
            throw new IllegalStateException(String.format("Subtable dropped rows;\n  Expected: %,d\n  Received: %,d",
                    origTbl.size(), startOffset));
        }
    }

    private static Table subTable(@NotNull final Table tbl, final long offset, final long length) {
        return tbl.tail(tbl.size() - offset).head(length);
    }

}
