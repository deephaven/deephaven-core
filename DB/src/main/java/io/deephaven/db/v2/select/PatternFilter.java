/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static io.deephaven.db.v2.select.MatchFilter.CaseSensitivity;
import static io.deephaven.db.v2.select.MatchFilter.MatchType;

/**
 * Base class for filters that use a Pattern to do filtering.
 */
public abstract class PatternFilter extends SelectFilterImpl {

    private static final long serialVersionUID = 1L;

    @NotNull
    protected final String columnName;
    protected final String value;
    protected final boolean invertMatch;
    final boolean caseInsensitive;

    protected Pattern pattern;

    public PatternFilter(MatchType matchType, String columnName, String value) {
        this(CaseSensitivity.MatchCase, matchType, columnName, value);
    }

    public PatternFilter(String columnName, String value) {
        this(MatchType.Regular, columnName, value);
    }

    public PatternFilter(CaseSensitivity sensitivity, MatchType matchType, @NotNull String columnName, String value) {
        this.columnName = columnName;
        this.value = value;
        this.caseInsensitive = (sensitivity == CaseSensitivity.IgnoreCase);
        this.invertMatch = (matchType == MatchType.Inverted);
    }

    @NotNull
    CaseSensitivity getCaseSensitivity() {
        return (caseInsensitive) ? CaseSensitivity.IgnoreCase : CaseSensitivity.MatchCase;
    }

    @NotNull
    MatchType getMatchType() {
        return invertMatch ? MatchType.Inverted : MatchType.Regular;
    }

    @NotNull
    public String getColumnName() {
        return columnName;
    }

    @Override
    public List<String> getColumns() {
        return Collections.singletonList(columnName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @Override
    public void init(TableDefinition tableDefinition) {
        synchronized (this) {
            if (pattern != null) {
                return;
            }

            final ColumnDefinition column = tableDefinition.getColumn(columnName);
            if (column == null) {
                throw new RuntimeException("Column \"" + columnName
                        + "\" doesn't exist in this table, available columns: " + tableDefinition.getColumnNames());
            }
            pattern = compile(value, caseInsensitive ? Pattern.CASE_INSENSITIVE : 0);
        }
    }

    protected abstract Pattern compile(String value, int flags);

    @Override
    public Index filter(Index selection, Index fullSet, Table table, boolean usePrev) {
        // noinspection unchecked
        final ColumnSource<String> columnSource = table.getColumnSource(columnName);

        if (invertMatch) {
            return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev,
                    (ChunkFilter.ObjectChunkFilter) (values, keys, results) -> {
                        results.setSize(0);
                        for (int ii = 0; ii < values.size(); ++ii) {
                            final String columnValue = (String) values.get(ii);
                            if (columnValue == null) {
                                continue;
                            }

                            if (!match(columnValue)) {
                                results.add(keys.get(ii));
                            }
                        }
                    });
        } else {
            return ChunkFilter.applyChunkFilter(selection, columnSource, usePrev,
                    (ChunkFilter.ObjectChunkFilter) (values, keys, results) -> {
                        results.setSize(0);
                        for (int ii = 0; ii < values.size(); ++ii) {
                            final String columnValue = (String) values.get(ii);
                            if (columnValue == null) {
                                continue;
                            }

                            if (match(columnValue)) {
                                results.add(keys.get(ii));
                            }
                        }
                    });
        }
    }

    protected abstract boolean match(String columnValue);

    @Override
    public boolean isSimpleFilter() {
        return true;
    }

    @Override
    public void setRecomputeListener(RecomputeListener listener) {}


    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        final PatternFilter that = (PatternFilter) o;
        return invertMatch == that.invertMatch &&
                caseInsensitive == that.caseInsensitive &&
                Objects.equals(columnName, that.columnName) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnName, value, invertMatch, caseInsensitive);
    }

    @Override
    public boolean canMemoize() {
        return true;
    }
}
