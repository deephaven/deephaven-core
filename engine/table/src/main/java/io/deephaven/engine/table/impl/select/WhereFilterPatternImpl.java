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
 * {@code inverted ^ pattern.matcher(value).matches()}.
 *
 * <p>
 * In the {@link Mode#FIND FIND} case, the logic is equivalent to {@code inverted ^ pattern.matcher(value).find()}.
 */
final class WhereFilterPatternImpl extends WhereFilterImpl {

    private static final long serialVersionUID = 1L;

    public static WhereFilterPatternImpl of(FilterPattern pattern, boolean inverted) {
        if (!(pattern.expression() instanceof ColumnName)) {
            throw new IllegalArgumentException("WhereFilterPatternImpl only supports filtering against a column name");
        }
        return new WhereFilterPatternImpl(pattern, inverted);
    }

    private final FilterPattern filterPattern;
    private final boolean inverted;
    private transient ObjectChunkFilter<String> chunkFilterImpl;

    private WhereFilterPatternImpl(FilterPattern filterPattern, boolean inverted) {
        this.filterPattern = Objects.requireNonNull(filterPattern);
        this.inverted = inverted;
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        final String columnName = columnName();
        final ColumnDefinition<?> column = tableDefinition.getColumn(columnName);
        if (column == null) {
            throw new RuntimeException(String.format("Column '%s' doesn't exist in this table, available columns: %s",
                    columnName, tableDefinition.getColumnNames()));
        }
        chunkFilterImpl = filterPattern.mode() == Mode.MATCHES
                ? (inverted ? new MatchesInverted() : new Matches())
                : (inverted ? new FindInverted() : new Find());
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        final ColumnSource<String> columnSource = table.getColumnSource(columnName());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilterImpl);
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
        return new WhereFilterPatternImpl(filterPattern, inverted);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterPatternImpl that = (WhereFilterPatternImpl) o;
        if (inverted != that.inverted)
            return false;
        return filterPattern.equals(that.filterPattern);
    }

    @Override
    public int hashCode() {
        int result = filterPattern.hashCode();
        result = 31 * result + (inverted ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "WhereFilterPatternImpl{" +
                "filterPattern=" + filterPattern +
                ", inverted=" + inverted +
                '}';
    }

    private String columnName() {
        return ((ColumnName) filterPattern.expression()).name();
    }

    class Matches implements ObjectChunkFilter<String> {
        @Override
        public void filter(
                ObjectChunk<String, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (matches(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    class MatchesInverted implements ObjectChunkFilter<String> {
        @Override
        public void filter(
                ObjectChunk<String, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (!matches(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    class Find implements ObjectChunkFilter<String> {
        @Override
        public void filter(
                ObjectChunk<String, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (find(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    class FindInverted implements ObjectChunkFilter<String> {
        @Override
        public void filter(
                ObjectChunk<String, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (!find(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }

    private boolean matches(String value) {
        return filterPattern.pattern().matcher(value).matches();
    }

    private boolean find(String value) {
        return filterPattern.pattern().matcher(value).find();
    }
}
