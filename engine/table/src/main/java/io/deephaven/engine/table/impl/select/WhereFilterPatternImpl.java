//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterPattern;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Objects;

final class WhereFilterPatternImpl extends WhereFilterImpl {

    private static final long serialVersionUID = 1L;

    static WhereFilter of(FilterPattern pattern) {
        if (!(pattern.expression() instanceof ColumnName)) {
            throw new IllegalArgumentException("WhereFilterPatternImpl only supports filtering against a column name");
        }
        return new WhereFilterPatternImpl(pattern);
    }

    private final FilterPattern filterPattern;
    private transient ObjectChunkFilter<CharSequence> chunkFilterImpl;
    private transient ObjectChunkFilter<CharSequence> chunkFilterInverseImpl;

    private WhereFilterPatternImpl(FilterPattern filterPattern) {
        this.filterPattern = Objects.requireNonNull(filterPattern);
    }

    @Override
    public void init(@NotNull final TableDefinition tableDefinition) {
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
        chunkFilterImpl = filter();
        chunkFilterInverseImpl = notFilter();
    }

    @NotNull
    @Override
    public WritableRowSet filter(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilterImpl);
    }

    @NotNull
    @Override
    public WritableRowSet filterInverse(
            @NotNull RowSet selection, @NotNull RowSet fullSet, @NotNull Table table, boolean usePrev) {
        final ColumnSource<?> columnSource = table.getColumnSource(columnName());
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, chunkFilterInverseImpl);
    }

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}

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

    private ObjectChunkFilter<CharSequence> filter() {
        switch (filterPattern.mode()) {
            case FIND:
                return filterPattern.invertPattern() ? new FindPatternInverted() : new Find();
            case MATCHES:
                return filterPattern.invertPattern() ? new MatchesPatternInverted() : new Matches();
            default:
                throw new IllegalStateException("Unexpected filter pattern mode " + filterPattern.mode());
        }
    }

    private ObjectChunkFilter<CharSequence> notFilter() {
        switch (filterPattern.mode()) {
            case FIND:
                return filterPattern.invertPattern() ? new NotFindPatternInverted() : new NotFind();
            case MATCHES:
                return filterPattern.invertPattern() ? new NotMatchesPatternInverted() : new NotMatches();
            default:
                throw new IllegalStateException("Unexpected filter pattern mode " + filterPattern.mode());
        }
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

    private final class Matches extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return WhereFilterPatternImpl.this.matches(value);
        }
    }

    private final class MatchesPatternInverted extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return WhereFilterPatternImpl.this.matchesPatternInverted(value);
        }
    }

    private final class Find extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return WhereFilterPatternImpl.this.find(value);
        }
    }

    private final class FindPatternInverted extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return WhereFilterPatternImpl.this.findPatternInverted(value);
        }
    }

    private final class NotMatches extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return !WhereFilterPatternImpl.this.matches(value);
        }
    }

    private final class NotMatchesPatternInverted extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return !WhereFilterPatternImpl.this.matchesPatternInverted(value);
        }
    }

    private final class NotFind extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return !WhereFilterPatternImpl.this.find(value);
        }
    }

    private final class NotFindPatternInverted extends ObjectChunkFilter<CharSequence> {
        @Override
        public boolean matches(CharSequence value) {
            return !WhereFilterPatternImpl.this.findPatternInverted(value);
        }
    }
}
