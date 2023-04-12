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
 * A filter based on {@link Matcher#matches()}.
 *
 * <p>
 * {@code (matchNulls && value == null) || (value != null && (invertPattern ^ pattern.matcher(value).matches()))}
 *
 * @see PatternFindFilter
 */
public final class PatternMatchesFilter extends WhereFilterImpl implements ObjectChunkFilter<String> {

    private static final long serialVersionUID = 1L;

    private final ColumnName columnName;
    private final Pattern pattern;
    private final boolean invertPattern;
    private final boolean matchNulls;

    public PatternMatchesFilter(ColumnName columnName, Pattern pattern, boolean invertPattern, boolean matchNulls) {
        this.pattern = Objects.requireNonNull(pattern);
        this.columnName = Objects.requireNonNull(columnName);
        this.invertPattern = invertPattern;
        this.matchNulls = matchNulls;
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
        return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev, this);
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
        return new PatternMatchesFilter(columnName, pattern, invertPattern, matchNulls);
    }

    /**
     * Creates a new filter that is the logical inversion of {@code this}.
     *
     * <p>
     * Equivalent to {@code new PatternMatchesFilter(columnName, pattern, !invertPattern, !matchNulls)}.
     *
     * @return the inverted filter
     */
    public PatternMatchesFilter invert() {
        return new PatternMatchesFilter(columnName, pattern, !invertPattern, !matchNulls);
    }

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

    private boolean matches(String value) {
        return (matchNulls && value == null) || (value != null && (invertPattern ^ pattern.matcher(value).matches()));
    }

    @Override
    public String toString() {
        return "PatternMatchesFilter{" +
                "columnName=" + columnName +
                ", pattern=" + pattern + " " + pattern.flags() +
                ", invertPattern=" + invertPattern +
                ", matchNulls=" + matchNulls +
                '}';
    }
}
