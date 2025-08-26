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
import io.deephaven.util.function.ThrowingConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Iterator;

public abstract class RowGroupInfo {
    private static final IterativeRowGroupInfo DEFAULT = new SingleRowGroup();

    /**
     * The default RowGroupInfo implementation. All data is within a single RowGroup
     *
     * @return a {@link RowGroupInfo} which uses only a single RowGroup
     */
    @ScriptApi
    public static RowGroupInfo singleRowGroup() {
        return DEFAULT;
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
    public static RowGroupInfo splitEvenly(final int numRowGroups) {
        if (numRowGroups == 1) {
            return singleRowGroup();
        } else if (numRowGroups < 1) {
            throw new IllegalArgumentException("Cannot define less than 1 RowGroup");
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

    /**
     * Provides an interface for iteratively applying a provided consumer to each RowGroup
     */
    private static abstract class IterativeRowGroupInfo extends RowGroupInfo implements Iterator<Table> {
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

        /**
         * Gets an iterator where each iteration identifies a RowGroup
         * 
         * @param input the pre-split table
         * @return an {@link Iterator}, where each iteration identifies a RowGroup
         */
        protected abstract Iterator<Table> getIterator(final @NotNull Table input);
    }

    /**
     * Keeps all rows within a single RowGroup
     */
    private static class SingleRowGroup extends IterativeRowGroupInfo {
        private Table input;

        @Override
        protected Iterator<Table> getIterator(final @NotNull Table input) {
            this.input = input;
            return this;
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

        @Override
        public void applyForRowGroups(final @NotNull Table input,
                final @NotNull ThrowingConsumer<Table, IOException> consumer) throws IOException {
            // no need to use the iterating version because we know we are writing only a single RowGroup
            consumer.accept(input);
        }
    }

    /**
     * Splits evenly across {@code numRowGroups} RowGroups
     */
    private static class SplitEvenly extends IterativeRowGroupInfo {
        private final long numRowGroups;

        private long impliedRowGroupSz;
        private long fractionalGroups;

        private long startOffset;
        private long nextIter;

        private Table input;

        private SplitEvenly(long numRowGroups) {
            this.numRowGroups = numRowGroups;
        }

        @Override
        public Iterator<Table> getIterator(final @NotNull Table input) {
            if (numRowGroups == 1) {
                return DEFAULT.getIterator(input);
            } else if (numRowGroups < 1) {
                throw new IllegalArgumentException("Cannot define less than 1 RowGroup");
            }

            this.input = input;

            this.impliedRowGroupSz = input.size() / numRowGroups;
            // number of groups which will have 1 additional row because rows are not evenly divisible by numRowGroups
            this.fractionalGroups = input.size() % numRowGroups;

            // forcing these to 0 means we can re-use this instance multiple times
            this.startOffset = this.nextIter = 0;

            return this;
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

    /**
     * Splits evenly across a number of RowGroups, ensuring that no group is larger than {@code maxRows}
     */
    private static class SplitByMaxRows extends IterativeRowGroupInfo {
        private final long maxRows;

        private SplitByMaxRows(long maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("MaxRows must be positive");
            }
            this.maxRows = maxRows;
        }

        @Override
        public Iterator<Table> getIterator(final @NotNull Table input) {
            final long numRowGroups = (input.size() / maxRows) + (input.size() % maxRows > 0 ? 1 : 0);
            if (numRowGroups == 1) {
                return DEFAULT.getIterator(input);
            } else {
                return new SplitEvenly(numRowGroups).getIterator(input);
            }
        }

        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException("SplitByMaxRows.hasNext() not implemented");
        }

        @Override
        public Table next() {
            throw new UnsupportedOperationException("SplitByMaxRows.next() not implemented");
        }
    }

    /**
     * Splits into RowGroups based on unique values within {@code groups}, and optionally further splits (per unique
     * {@code groups} ensuring that no group is larger than {@code maxRows}. If {@code maxRows} is not desired, this
     * parameter may be set to {@code Long.MAX_VALUE}
     */
    private static class SplitByGroups extends IterativeRowGroupInfo {
        private final long maxRows;
        private final String[] groups;
        private final IterativeRowGroupInfo subSplitInfo;

        private Table[] partitionedTables;
        private int nextTable;

        private Iterator<Table> subIter;

        private SplitByGroups(long maxRows, final @NotNull String[] groups) {
            this.maxRows = maxRows;
            this.groups = groups;
            this.subSplitInfo = maxRows == Long.MAX_VALUE ? DEFAULT : new SplitByMaxRows(maxRows);
        }

        @Override
        public Iterator<Table> getIterator(final @NotNull Table input) {
            final Table[] newPartitions = input.partitionBy(groups).constituents();
            ensureConsistentOrdering(input, newPartitions);

            reset();
            this.partitionedTables = newPartitions;

            return this;
        }

        private void reset() {
            this.partitionedTables = null;
            this.nextTable = 0;
            this.subIter = null;
        }

        @Override
        public boolean hasNext() {
            final boolean hasNext;
            if (partitionedTables == null) {
                hasNext = false;
            } else {
                hasNext = (nextTable < partitionedTables.length) || (subIter != null && subIter.hasNext());
            }

            if (!hasNext) {
                reset();
            }

            return hasNext;
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
            subIter = subSplitInfo.getIterator(subTable);
            return subIter.next();
        }
    }

    /**
     * FIXME: we know this can be potentially incorrect in some cases ... ? Ensure that table-ordering has not changed
     *
     * @param origTbl the original (pre-split) table
     * @param ordered the array-split table
     */
    private static void ensureConsistentOrdering(final @NotNull Table origTbl, final @NotNull Table[] ordered) {
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
