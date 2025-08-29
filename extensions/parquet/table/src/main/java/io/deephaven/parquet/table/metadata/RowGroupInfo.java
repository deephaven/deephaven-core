//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.function.ThrowingConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public abstract class RowGroupInfo {
    private static IterativeRowGroupInfo DEFAULT;

    /**
     * Returns the DEFAULT instance
     *
     * @return the DEFAULT instance
     */
    private synchronized static IterativeRowGroupInfo getDefault() {
        if (DEFAULT == null) {
            DEFAULT = new SingleRowGroup();
        }

        return DEFAULT;
    }

    /**
     * The default RowGroupInfo implementation. All data is within a single RowGroup
     *
     * @return a {@link RowGroupInfo} which uses only a single RowGroup
     */
    @ScriptApi
    public static RowGroupInfo singleRowGroup() {
        return getDefault();
    }

    /**
     * Split evenly into a pre-defined number of RowGroups, each of which contains the same number of rows as each
     * other. If the input table size is not evenly divisible by the number of RowGroups requested, then a number of
     * RowGroups will contain 1 fewer row
     *
     * @param numRowGroups the number of RowGroups to write
     * @return A {@link RowGroupInfo} which splits the input into a pre-defined number of RowGroups
     */
    @ScriptApi
    public static RowGroupInfo splitEvenly(final long numRowGroups) {
        if (numRowGroups == 1) {
            return singleRowGroup();
        } else {
            return new SplitEvenly(numRowGroups);
        }
    }

    /**
     * Splits into a number of RowGroups, each of which has no more than {@code maxRows} rows.
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
        return byGroup(Long.MAX_VALUE, groups);
    }

    /**
     * Splits each unique group into a number of RowGroups. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the `writeTable(...)` call. If a given RowGroup yields a
     * row count greater than {@code maxRows}, then it will be split further using
     * {@link RowGroupInfo#withMaxRows(long)}
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
     * Splits the {@code input} into a number of RowGroups and applies a provided consumer to each
     *
     * @param input the pre-split table to write
     * @param consumer the consumer which is used to write each individual RowGroup
     * @throws IOException if the underlying {@code consumer} throws an {@link IOException}
     */
    public abstract void applyForRowGroups(final @NotNull Table input,
            final @NotNull ThrowingConsumer<Table, IOException> consumer)
            throws IOException;

    public abstract <T> T walk(final @NotNull Visitor<T> visitor);

    /**
     * Provides an interface for iteratively applying a provided consumer to each RowGroup
     */
    private static abstract class IterativeRowGroupInfo extends RowGroupInfo {
        /**
         * Gets an iterator where each iteration identifies a RowGroup
         *
         * @param input the pre-split table
         * @return an {@link Iterator}, where each iteration identifies a RowGroup
         */
        protected abstract Iterator<Table> getIterator(final @NotNull Table input);

        @Override
        public void applyForRowGroups(final @NotNull Table input,
                final @NotNull ThrowingConsumer<Table, IOException> consumer)
                throws IOException {
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                final Iterator<Table> iter = getIterator(input);
                while (iter.hasNext()) {
                    consumer.accept(iter.next());
                }
            }
        }
    }

    /**
     * Keeps all rows within a single RowGroup
     */
    public static class SingleRowGroup extends IterativeRowGroupInfo {
        private SingleRowGroup() {
            // should not be directly instantiated by users
        }

        @Override
        public void applyForRowGroups(final @NotNull Table input,
                final @NotNull ThrowingConsumer<Table, IOException> consumer) throws IOException {
            // no need to use the iterating version because we know we have only a single RowGroup
            consumer.accept(input);
        }

        @Override
        public <T> T walk(@NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        protected Iterator<Table> getIterator(final @NotNull Table input) {
            return new SingleRowGroupIterator(input);
        }

        /**
         * yes, it seems silly that we have an Iterator for a (known) single element. BUT, this allows other Iterative
         * implementations to defer to the "single" implementation when they know there will be only one RowGroup
         */
        private static class SingleRowGroupIterator implements Iterator<Table> {
            private Table input;

            private SingleRowGroupIterator(Table input) {
                this.input = input;
            }

            @Override
            public boolean hasNext() {
                return input != null;
            }

            @Override
            public Table next() {
                try {
                    return input;
                } finally {
                    input = null;
                }
            }
        }
    }

    /**
     * Splits evenly across {@code numRowGroups} RowGroups
     */
    public static class SplitEvenly extends IterativeRowGroupInfo {
        private final long numRowGroups;

        private SplitEvenly(long numRowGroups) {
            if (numRowGroups < 1) {
                throw new IllegalArgumentException("Cannot define less than 1 RowGroup");
            }
            this.numRowGroups = numRowGroups;
        }

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }

        long getNumRowGroups() {
            return numRowGroups;
        }

        @Override
        protected Iterator<Table> getIterator(final @NotNull Table input) {
            return numRowGroups == 1 ? getDefault().getIterator(input) : new SplitEvenlyIterator(input);
        }

        private class SplitEvenlyIterator implements Iterator<Table> {
            private final Table input;

            private long impliedRowGroupSz;
            private long fractionalGroups;

            private long startOffset = 0;
            private long nextIter = 0;

            private SplitEvenlyIterator(final @NotNull Table input) {
                this.input = input;

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
                try {
                    final RowSet rawRowSet = input.getRowSet();

                    final long nextSz = impliedRowGroupSz + (nextIter < fractionalGroups ? 1 : 0);
                    final WritableRowSet nextRows = rawRowSet.subSetByPositionRange(startOffset, startOffset += nextSz);
                    return input.getSubTable(nextRows.toTracking());
                } finally {
                    nextIter++;
                }
            }
        }
    }

    /**
     * Splits evenly across a number of RowGroups, ensuring that no group is larger than {@code maxRows}
     */
    public static class SplitByMaxRows extends RowGroupInfo {
        private final long maxRows;

        private SplitByMaxRows(long maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("MaxRows must be positive");
            }
            this.maxRows = maxRows;
        }

        @Override
        public void applyForRowGroups(final @NotNull Table input,
                final @NotNull ThrowingConsumer<Table, IOException> consumer) throws IOException {
            getIterativeRowGroupImpl(maxRows, input).applyForRowGroups(input, consumer);
        }

        @Override
        public <T> T walk(@NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }

        long getMaxRows() {
            return maxRows;
        }

        private static IterativeRowGroupInfo getIterativeRowGroupImpl(long maxRows, final @NotNull Table input) {
            final long numRowGroups = (input.size() / maxRows) + (input.size() % maxRows > 0 ? 1 : 0);
            return new SplitEvenly(numRowGroups);
        }
    }

    /**
     * Splits into RowGroups based on unique values within {@code groups}, and optionally further splits (per unique
     * {@code groups} ensuring that no group is larger than {@code maxRows}. If {@code maxRows} is not desired, this
     * parameter may be set to {@code Long.MAX_VALUE}
     */
    public static class SplitByGroups extends IterativeRowGroupInfo {
        private final long maxRows;
        private final String[] groups;

        private SplitByGroups(long maxRows, final @NotNull String[] groups) {
            this.maxRows = maxRows;
            this.groups = groups;
        }

        @Override
        public <T> T walk(@NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }

        long getMaxRows() {
            return maxRows;
        }

        String[] getGroups() {
            return groups;
        }

        @Override
        protected Iterator<Table> getIterator(final @NotNull Table input) {
            return new SplitByGroupsIterator(input);
        }

        private class SplitByGroupsIterator implements Iterator<Table> {
            private final Table[] partitionedTables;

            private int nextTable;
            private Iterator<Table> subIter;

            private SplitByGroupsIterator(final @NotNull Table input) {
                ensureOrderedForGrouping(input);
                this.partitionedTables = input.partitionBy(groups).constituents();
                this.nextTable = 0;
            }

            @Override
            public boolean hasNext() {
                return (nextTable < partitionedTables.length) || (subIter != null && subIter.hasNext());
            }

            @Override
            public Table next() {
                // if we're already working on a split sub-table, let the sub-table splitter handle this request
                if (subIter != null && subIter.hasNext()) {
                    return subIter.next();
                }

                // else we've moved on to the next partitioned table
                final Table subTable = partitionedTables[nextTable++];
                if (subTable.size() <= maxRows) {
                    // no need to split this one
                    return subTable;
                }

                // get a new sub-table iterator for this partitioned table, and return the first sub-table
                subIter = SplitByMaxRows.getIterativeRowGroupImpl(maxRows, subTable).getIterator(subTable);
                return subIter.next();
            }
        }

        /**
         * Ensure that grouping will not change row-ordering
         *
         * @param input the table to be checked
         */
        private void ensureOrderedForGrouping(final @NotNull Table input) {
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

    public interface Visitor<T> {
        T visit(final @NotNull RowGroupInfo.SingleRowGroup single);

        T visit(final @NotNull RowGroupInfo.SplitEvenly splitEvenly);

        T visit(final @NotNull RowGroupInfo.SplitByMaxRows withMaxRows);

        T visit(final @NotNull RowGroupInfo.SplitByGroups byGroups);

        T visit(final @NotNull RowGroupInfo generic);
    }
}
