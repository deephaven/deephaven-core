/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.ObjectChunkFilter;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A filter based on a regular-expression {@link Pattern}.
 *
 * <p>
 * In the {@link Mode#MATCHES MATCHES} case, the logic is equivalent to
 * {@code value != null && (invertPattern ^ pattern.matcher(value).matches())}.
 *
 * <p>
 * In the {@link Mode#FIND FIND} case, the logic is equivalent to
 * {@code value != null && (invertPattern ^ pattern.matcher(value).find())}.
 *
 * <p>
 * Note: this filter never matches against a {@code null} value.
 */
public final class PatternFilter extends WhereFilterImpl {

    private static final long serialVersionUID = 1L;

    // Update to table-api structs in https://github.com/deephaven/deephaven-core/pull/3441
    public enum Mode {
        /**
         * Matches the entire {@code input} against the {@code pattern}, uses {@link Matcher#matches()}.
         */
        MATCHES,

        /**
         * Matches any subsequence of the {@code input} against the {@code pattern}, uses {@link Matcher#find()}.
         */
        FIND
    }

    private final ColumnName columnName;
    private final Pattern pattern;
    private final Mode mode;
    private final boolean invertPattern;

    /**
     * Creates a new pattern filter.
     *
     * @param columnName the column name
     * @param pattern the pattern
     * @param mode the mode
     * @param invertPattern if the pattern result should be inverted
     */
    public PatternFilter(ColumnName columnName, Pattern pattern, Mode mode, boolean invertPattern) {
        this.columnName = Objects.requireNonNull(columnName);
        this.pattern = Objects.requireNonNull(pattern);
        this.mode = Objects.requireNonNull(mode);
        this.invertPattern = invertPattern;
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        final ColumnDefinition<?> column = tableDefinition.getColumn(columnName.name());
        if (column == null) {
            throw new RuntimeException(String.format("Column '%s' doesn't exist in this table, available columns: %s",
                    columnName.name(), tableDefinition.getColumnNames()));
        }
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        final ColumnSource<String> columnSource = table.getColumnSource(columnName.name());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilter());
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {

    }

    @Override
    public boolean canMemoize() {
        return true;
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(columnName.name());
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public WhereFilter copy() {
        return new PatternFilter(columnName, pattern, mode, invertPattern);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PatternFilter that = (PatternFilter) o;
        if (invertPattern != that.invertPattern)
            return false;
        if (!columnName.equals(that.columnName))
            return false;
        if (!patternEquals(pattern, that.pattern))
            return false;
        return mode == that.mode;
    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + patternHashcode(pattern);
        result = 31 * result + mode.hashCode();
        result = 31 * result + (invertPattern ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "PatternFilter{" +
                "columnName=" + columnName +
                ", pattern=" + pattern.pattern() + " " + pattern.flags() +
                ", mode=" + mode +
                ", invertPattern=" + invertPattern +
                '}';
    }

    private ObjectChunkFilter<String> chunkFilter() {
        return mode == Mode.MATCHES
                ? (invertPattern
                        ? new MatchesInvertedPattern()
                        : new Matches())
                : (invertPattern
                        ? new FindInvertedPattern()
                        : new Find());
    }

    class Matches implements ObjectChunkFilter<String> {
        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (matches(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    class MatchesInvertedPattern implements ObjectChunkFilter<String> {
        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (matchesInvertedPattern(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    class Find implements ObjectChunkFilter<String> {
        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (find(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    class FindInvertedPattern implements ObjectChunkFilter<String> {
        @Override
        public void filter(ObjectChunk<String, ? extends Values> values, LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (findInvertedPattern(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    private boolean matches(String value) {
        return value != null && pattern.matcher(value).matches();
    }

    private boolean matchesInvertedPattern(String value) {
        return value != null && !pattern.matcher(value).matches();
    }

    private boolean find(String value) {
        return value != null && pattern.matcher(value).find();
    }

    private boolean findInvertedPattern(String value) {
        return value != null && !pattern.matcher(value).find();
    }

    private static boolean patternEquals(Pattern x, Pattern y) {
        return x.flags() == y.flags() && x.pattern().equals(y.pattern());
    }

    private static int patternHashcode(Pattern x) {
        return 31 * x.pattern().hashCode() + x.flags();
    }
}
