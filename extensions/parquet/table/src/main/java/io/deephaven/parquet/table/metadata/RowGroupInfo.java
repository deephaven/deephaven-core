//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

public interface RowGroupInfo {
    /**
     * The default RowGroupInfo implementation. All data is within a single RowGroup
     *
     * @return a {@link RowGroupInfo} which uses only a single RowGroup
     */
    @ScriptApi
    static RowGroupInfo singleGroup() {
        return SingleGroup.SINGLE_ROW_GROUP;
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
    static RowGroupInfo maxGroups(final long numRowGroups) {
        return new MaxGroups(numRowGroups);
    }

    /**
     * Splits into a number of RowGroups, each of which has no more than {@code maxRows} rows.
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @return a {@link RowGroupInfo} which splits the input into a number of RowGroups, each containing no more than
     *         {@code maxRows} rows
     */
    @ScriptApi
    static RowGroupInfo maxRows(final long maxRows) {
        return new MaxRows(maxRows);
    }

    /**
     * Splits each unique group into a RowGroup. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroups(final String... groups) {
        return byGroups(Long.MAX_VALUE, groups);
    }

    /**
     * Splits each unique group into a RowGroup. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call
     *
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a single RowGroup per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroups(final List<String> groups) {
        return byGroups(Long.MAX_VALUE, groups);
    }

    /**
     * Splits each unique group into a number of RowGroups. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call. If a given RowGroup
     * yields a row count greater than {@code maxRows}, then it will be split further using
     * {@link RowGroupInfo#maxRows(long)}
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a number of RowGroups per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroups(final long maxRows, final String... groups) {
        return new ByGroups(maxRows, groups);
    }

    /**
     * Splits each unique group into a number of RowGroups. If the input table does not have all values for the group(s)
     * contiguously, then an exception will be thrown during the {@code writeTable(...)} call. If a given RowGroup
     * yields a row count greater than {@code maxRows}, then it will be split further using
     * {@link RowGroupInfo#maxRows(long)}
     *
     * @param maxRows the maximum number of rows in each RowGroup
     * @param groups Grouping column name(s)
     * @return a {@link RowGroupInfo} which includes a number of RowGroups per unique grouping-value
     */
    @ScriptApi
    static RowGroupInfo byGroups(final long maxRows, final List<String> groups) {
        return byGroups(maxRows, groups.toArray(new String[0]));
    }

    <T> T walk(final @NotNull Visitor<T> visitor);

    /**
     * Keeps all rows within a single RowGroup
     */
    enum SingleGroup implements RowGroupInfo {
        SINGLE_ROW_GROUP;

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            return visitor.visit(this);
        }
    }

    /**
     * Splits evenly across {@code numRowGroups} RowGroups
     */
    final class MaxGroups implements RowGroupInfo {
        private final long numRowGroups;

        private MaxGroups(long numRowGroups) {
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
            if (obj instanceof MaxGroups) {
                return ((MaxGroups) obj).getNumRowGroups() == getNumRowGroups();
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("%s{numRowGroups=%d}", MaxGroups.class.getSimpleName(), getNumRowGroups());
        }

        public long getNumRowGroups() {
            return numRowGroups;
        }
    }

    /**
     * Splits evenly across a number of RowGroups, ensuring that no group is larger than {@code maxRows}
     */
    final class MaxRows implements RowGroupInfo {
        private final long maxRows;

        private MaxRows(long maxRows) {
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
            if (obj instanceof MaxRows) {
                return ((MaxRows) obj).getMaxRows() == getMaxRows();
            }
            return false;
        }

        @Override
        public String toString() {
            return String.format("%s{maxRows=%d}", MaxRows.class.getSimpleName(), getMaxRows());
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
    final class ByGroups implements RowGroupInfo {
        private final long maxRows;
        private final String[] groups;

        private ByGroups(long maxRows, final @NotNull String[] groups) {
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
            if (obj instanceof ByGroups) {
                final ByGroups other = (ByGroups) obj;
                return other.getMaxRows().equals(getMaxRows()) && other.getGroups().equals(getGroups());
            }
            return false;
        }

        @Override
        public String toString() {
            if (maxRows == Long.MAX_VALUE) {
                return String.format("%s{groups=%s}", ByGroups.class.getSimpleName(),
                        Arrays.toString(getGroups().toArray()));
            } else {
                return String.format("%s{maxRows=%d, groups=%s}", ByGroups.class.getSimpleName(), maxRows,
                        Arrays.toString(getGroups().toArray()));
            }
        }

        public OptionalLong getMaxRows() {
            return (maxRows == Long.MAX_VALUE) ? OptionalLong.empty() : OptionalLong.of(maxRows);
        }

        public List<String> getGroups() {
            return Arrays.stream(groups).collect(Collectors.toUnmodifiableList());
        }
    }

    interface Visitor<T> {
        T visit(final @NotNull RowGroupInfo.SingleGroup single);

        T visit(final @NotNull RowGroupInfo.MaxGroups maxGroups);

        T visit(final @NotNull RowGroupInfo.MaxRows maxRows);

        T visit(final @NotNull RowGroupInfo.ByGroups byGroups);
    }
}
