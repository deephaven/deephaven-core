//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public interface RowGroupInfo {
    /**
     * The default RowGroupInfo implementation. All data is within a single RowGroup
     *
     * @return a {@link RowGroupInfo} which uses only a single RowGroup
     */
    @ScriptApi
    static RowGroupInfo singleRowGroup() {
        return SingleRowGroup.SINGLE_ROW_GROUP;
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
    static RowGroupInfo splitEvenly(final long numRowGroups) {
        return new SplitEvenly(numRowGroups);
    }

    /**
     * Splits into a number of RowGroups, each of which has no more than {@code maxRows} rows.
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @return a {@link RowGroupInfo} which splits the input into a number of RowGroups, each containing no more than
     *         {@code maxRows} rows
     */
    @ScriptApi
    static RowGroupInfo withMaxRows(final long maxRows) {
        return new SplitByMaxRows(maxRows);
    }

    /**
     * Splits each unique group into a RowGroup. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroup(final String... groups) {
        return byGroup(Long.MAX_VALUE, groups);
    }

    /**
     * Splits each unique group into a RowGroup. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroup(final List<String> groups) {
        return byGroup(Long.MAX_VALUE, groups);
    }

    /**
     * Splits each unique group into a number of RowGroups. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call. If a given RowGroup
     * yields a row count greater than {@code maxRows}, then it will be split further using
     * {@link RowGroupInfo#withMaxRows(long)}
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a number of RowGroups per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroup(final long maxRows, final String... groups) {
        return new SplitByGroups(maxRows, groups);
    }

    /**
     * Splits each unique group into a number of RowGroups. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call. If a given RowGroup
     * yields a row count greater than {@code maxRows}, then it will be split further using
     * {@link RowGroupInfo#withMaxRows(long)}
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a number of RowGroups per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroup(final long maxRows, final List<String> groups) {
        return byGroup(maxRows, groups.toArray(new String[0]));
    }

    <T> T walk(final @NotNull Visitor<T> visitor);

    /**
     * Keeps all rows within a single RowGroup
     */
    enum SingleRowGroup implements RowGroupInfo {
        SINGLE_ROW_GROUP;

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * Splits evenly across {@code numRowGroups} RowGroups
     */
    final class SplitEvenly implements RowGroupInfo {
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

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof SplitEvenly) {
                return ((SplitEvenly) obj).getNumRowGroups() == getNumRowGroups();
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("%s{numRowGroups=%d}", SplitEvenly.class.getSimpleName(), getNumRowGroups());
        }

        public long getNumRowGroups() {
            return numRowGroups;
        }
    }

    /**
     * Splits evenly across a number of RowGroups, ensuring that no group is larger than {@code maxRows}
     */
    final class SplitByMaxRows implements RowGroupInfo {
        private final long maxRows;

        private SplitByMaxRows(long maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("MaxRows must be positive");
            }
            this.maxRows = maxRows;
        }

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof SplitByMaxRows) {
                return ((SplitByMaxRows) obj).getMaxRows() == getMaxRows();
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("%s{maxRows=%d}", SplitByMaxRows.class.getSimpleName(), getMaxRows());
        }

        public long getMaxRows() {
            return maxRows;
        }
    }

    /**
     * Splits into RowGroups based on unique values within {@code groups}, and optionally further splits (per unique
     * {@code groups} ensuring that no group is larger than {@code maxRows}. If {@code maxRows} is not desired, this
     * parameter may be set to {@code Long.MAX_VALUE}
     */
    final class SplitByGroups implements RowGroupInfo {
        private final long maxRows;
        private final String[] groups;

        private SplitByGroups(long maxRows, final @NotNull String[] groups) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("MaxRows must be positive");
            } else if (groups.length == 0) {
                throw new IllegalArgumentException("Must contain at least one group");
            }
            this.maxRows = maxRows;
            this.groups = groups;
        }

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof SplitByGroups) {
                final SplitByGroups other = (SplitByGroups) obj;
                return other.getMaxRows() == getMaxRows() && other.getGroups().equals(getGroups());
            }
            return false;
        }

        @Override
        public String toString() {
            if (getMaxRows() == Long.MAX_VALUE) {
                return String.format("%s{groups=%s}", SplitByGroups.class.getSimpleName(),
                        Arrays.toString(getGroups().toArray()));
            } else {
                return String.format("%s{maxRows=%d, groups=%s}", SplitByGroups.class.getSimpleName(), getMaxRows(),
                        Arrays.toString(getGroups().toArray()));
            }
        }

        public long getMaxRows() {
            return maxRows;
        }

        public List<String> getGroups() {
            return Arrays.stream(groups).collect(Collectors.toUnmodifiableList());
        }
    }

    interface Visitor<T> {
        T visit(final @NotNull RowGroupInfo.SingleRowGroup single);

        T visit(final @NotNull RowGroupInfo.SplitEvenly splitEvenly);

        T visit(final @NotNull RowGroupInfo.SplitByMaxRows withMaxRows);

        T visit(final @NotNull RowGroupInfo.SplitByGroups byGroups);
    }
}
