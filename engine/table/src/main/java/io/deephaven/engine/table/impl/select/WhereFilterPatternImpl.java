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

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

final class WhereFilterPatternImpl extends WhereFilterImpl {

    private static final long serialVersionUID = 1L;

    static WhereFilter of(FilterPattern pattern, boolean inverted) {
        if (!(pattern.expression() instanceof ColumnName)) {
            throw new IllegalArgumentException("WhereFilterPatternImpl only supports filtering against a column name");
        }
        final WhereFilterPatternImpl impl = new WhereFilterPatternImpl(pattern);
        return inverted ? new WhereFilterInvertedImpl(impl) : impl;
    }

    private final FilterPattern filterPattern;
    private transient ObjectChunkFilter<CharSequence> chunkFilterImpl;
    private transient ObjectChunkFilter<CharSequence> chunkFilterInverseImpl;

    private WhereFilterPatternImpl(FilterPattern filterPattern) {
        this.filterPattern = Objects.requireNonNull(filterPattern);
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        final String columnName = columnName();
        final ColumnDefinition<?> column = tableDefinition.getColumn(columnName);
        if (column == null) {
            throw new RuntimeException(String.format("Column '%s' doesn't exist in this table, available columns: %s",
                    columnName, tableDefinition.getColumnNames()));
        }
        if (!CharSequence.class.isAssignableFrom(column.getDataType())) {
            throw new RuntimeException(
                    String.format("Column '%s', type %s, is not a CharSequence", columnName, column.getDataType()));
        }
        final Predicate<CharSequence> p = predicate();
        chunkFilterImpl = new ObjectChunkFilterPredicate<>(p);
        chunkFilterInverseImpl = new ObjectChunkFilterPredicate<>(p.negate());
    }

    @Override
    public WritableRowSet filter(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilterImpl);
    }

    @Override
    public WritableRowSet filterInverse(RowSet selection, RowSet fullSet, Table table, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilterInverseImpl);
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
        return List.of(columnName());
    }

    @Override
    public List<String> getColumnArrays() {
        return List.of();
    }

    @Override
    public WhereFilter copy() {
        return new WhereFilterPatternImpl(filterPattern);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WhereFilterPatternImpl that = (WhereFilterPatternImpl) o;
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

    private String columnName() {
        return ((ColumnName) filterPattern.expression()).name();
    }

    private Predicate<CharSequence> predicate() {
        final Predicate<CharSequence> p;
        switch (filterPattern.mode()) {
            case FIND:
                p = filterPattern.invertPattern() ? this::findPatternInverted : this::find;
                break;
            case MATCHES:
                p = filterPattern.invertPattern() ? this::matchesPatternInverted : this::matches;
                break;
            default:
                throw new IllegalStateException("Unexpected filter pattern mode " + filterPattern.mode());
        }
        return p;
    }

    private boolean matches(CharSequence value) {
        return value != null && filterPattern.pattern().matcher(value).matches();
    }

    private boolean matchesPatternInverted(CharSequence value) {
        return value != null && !filterPattern.pattern().matcher(value).matches();
    }

    private boolean find(CharSequence value) {
        return value != null && filterPattern.pattern().matcher(value).find();
    }

    private boolean findPatternInverted(CharSequence value) {
        return value != null && !filterPattern.pattern().matcher(value).find();
    }

    private static class ObjectChunkFilterPredicate<T> implements ObjectChunkFilter<T> {
        private final Predicate<T> predicate;

        public ObjectChunkFilterPredicate(Predicate<T> predicate) {
            this.predicate = Objects.requireNonNull(predicate);
        }

        @Override
        public void filter(
                ObjectChunk<T, ? extends Values> values,
                LongChunk<OrderedRowKeys> keys,
                WritableLongChunk<OrderedRowKeys> results) {
            results.setSize(0);
            for (int ix = 0; ix < values.size(); ++ix) {
                if (predicate.test(values.get(ix))) {
                    results.add(keys.get(ix));
                }
            }
        }
    }
}
