//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.util.TableTools.merge;

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
     * Splits each unique group into a RowGroup
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    public static RowGroupInfo byGroup(final String... groups) {
        return new SplitByGroups(groups);
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
                final long tailSz = input.size() - startOffset;
                final long headSz = suggestedRowGroupSize + (ii < fractionalGroups ? 1 : 0);
                ret[ii] = input.tail(tailSz).head(headSz);
                startOffset += headSz;
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
                final long tailSz = input.size() - startOffset;
                final long headSz = maxRows;
                ret[ii] = input.tail(tailSz).head(headSz);
                startOffset += headSz;
            }

            return ret;
        }
    }

    private static class SplitByGroups extends RowGroupInfo {
        private final String[] groups;

        private SplitByGroups(final String[] groups) {
            if (groups == null || groups.length == 0) {
                throw new IllegalArgumentException("Requires at lease one group");
            }
            this.groups = groups;
        }

        @Override
        public Table[] splitForRowGroups(@NotNull final Table input) {
            final Table[] grouped = input.partitionBy(groups).constituents();
            final String diff = TableTools.diff(merge(grouped), input, 1);
            if (!diff.isEmpty()) {
                throw new IllegalStateException("Grouped value(s) must be contiguous; " + diff);
            }
            return grouped;
        }
    }
}
