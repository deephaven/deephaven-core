/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.api.filter.FilterPattern.Mode;
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

    private final FilterPattern filterPattern;

    public PatternFilter(FilterPattern filterPattern) {
        if (!(filterPattern.expression() instanceof ColumnName)) {
            throw new IllegalArgumentException("PatternFilter only supports filtering against a column name");
        }
        this.filterPattern = Objects.requireNonNull(filterPattern);
    }

    /**
     * Creates a new pattern filter.
     *
     * @param columnName the column name
     * @param pattern the pattern
     * @param mode the mode
     * @param invertPattern if the pattern result should be inverted
     */
    public PatternFilter(ColumnName columnName, Pattern pattern, Mode mode, boolean invertPattern) {
        this(FilterPattern.builder()
                .expression(columnName)
                .pattern(pattern)
                .mode(mode)
                .invertPattern(invertPattern)
                .build());
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        final String columnName = columnName();
        final ColumnDefinition<?> column = tableDefinition.getColumn(columnName);
        if (column == null) {
            throw new RuntimeException(String.format("Column '%s' doesn't exist in this table, available columns: %s",
                    columnName, tableDefinition.getColumnNames()));
        }
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        final ColumnSource<String> columnSource = table.getColumnSource(columnName());
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
        return Collections.singletonList(columnName());
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public WhereFilter copy() {
        return new PatternFilter(filterPattern);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        PatternFilter that = (PatternFilter) o;
        return filterPattern.equals(that.filterPattern);
    }

    @Override
    public int hashCode() {
        return filterPattern.hashCode();
    }

    @Override
    public String toString() {
        return filterPattern.toString();
    }

    private ObjectChunkFilter<String> chunkFilter() {
        return filterPattern.mode() == Mode.MATCHES
                ? (filterPattern.invertPattern()
                        ? new MatchesInvertedPattern()
                        : new Matches())
                : (filterPattern.invertPattern()
                        ? new FindInvertedPattern()
                        : new Find());
    }

    private String columnName() {
        return ((ColumnName) filterPattern.expression()).name();
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
        return value != null && filterPattern.pattern().matcher(value).matches();
    }

    private boolean matchesInvertedPattern(String value) {
        return value != null && !filterPattern.pattern().matcher(value).matches();
    }

    private boolean find(String value) {
        return value != null && filterPattern.pattern().matcher(value).find();
    }

    private boolean findInvertedPattern(String value) {
        return value != null && !filterPattern.pattern().matcher(value).find();
    }
}
