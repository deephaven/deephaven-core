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
import io.deephaven.engine.table.impl.select.MatchFilter.CaseSensitivity;
import io.deephaven.engine.table.impl.select.MatchFilter.MatchType;
import io.deephaven.engine.util.string.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A filter based on {@link Matcher#find()}.
 *
 * <p>
 * {@code (matchNulls && value == null) || (value != null && (invertPattern ^ pattern.matcher(value).find()))}
 *
 * @see PatternMatchesFilter
 */
public final class PatternFindFilter extends WhereFilterImpl implements ObjectChunkFilter<String> {

    private static final long serialVersionUID = 1L;

    public static PatternFindFilter stringContainsFilter(
            MatchType matchType,
            String columnName,
            String... values) {
        return stringContainsFilter(CaseSensitivity.MatchCase, matchType, columnName, values);
    }

    public static PatternFindFilter stringContainsFilter(
            String columnName,
            String... values) {
        return stringContainsFilter(MatchType.Regular, columnName, values);
    }

    public static PatternFindFilter stringContainsFilter(
            CaseSensitivity sensitivity,
            MatchType matchType,
            @NotNull String columnName,
            String... values) {
        return stringContainsFilter(sensitivity, matchType, columnName, true, false, values);
    }

    public static PatternFindFilter stringContainsFilter(
            CaseSensitivity sensitivity,
            MatchType matchType,
            @NotNull String columnName,
            boolean internalDisjuntive,
            boolean removeQuotes,
            String... values) {
        final String value = constructRegex(values, matchType, internalDisjuntive, removeQuotes, columnName);
        // note: this is persisting old behavior w/ matchNulls = false
        return new PatternFindFilter(
                ColumnName.of(columnName),
                Pattern.compile(value, sensitivity == CaseSensitivity.IgnoreCase ? Pattern.CASE_INSENSITIVE : 0),
                matchType == MatchType.Inverted,
                false);
    }

    private final ColumnName columnName;
    private final Pattern pattern;
    private final boolean invertPattern;
    private final boolean matchNulls;

    public PatternFindFilter(ColumnName columnName, Pattern pattern, boolean invertPattern, boolean matchNulls) {
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
        return new PatternFindFilter(columnName, pattern, invertPattern, matchNulls);
    }

    /**
     * Creates a new filter that is the logical inversion of {@code this}.
     *
     * <p>
     * Equivalent to {@code new PatternFindFilter(columnName, pattern, !invertPattern, !matchNulls)}.
     *
     * @return the inverted filter
     */
    public PatternFindFilter invert() {
        return new PatternFindFilter(columnName, pattern, !invertPattern, !matchNulls);
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
        return (matchNulls && value == null) || (value != null && (invertPattern ^ pattern.matcher(value).find()));
    }

    @Override
    public String toString() {
        return "PatternFindFilter{" +
                "columnName=" + columnName +
                ", pattern=" + pattern + " " + pattern.flags() +
                ", invertPattern=" + invertPattern +
                ", matchNulls=" + matchNulls +
                '}';
    }

    private static String constructRegex(String[] values, MatchType matchType, boolean internalDisjunctive,
            boolean removeQuotes, String columnName) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException(
                    "StringContainsFilter must be created with at least one value parameter");
        }
        final MatchFilter.ColumnTypeConvertor converter = removeQuotes
                ? MatchFilter.ColumnTypeConvertorFactory.getConvertor(String.class, columnName)
                : null;
        final String regex;
        final Stream<String> valueStream = Arrays.stream(values)
                .map(val -> {
                    if (StringUtils.isNullOrEmpty(val)) {
                        throw new IllegalArgumentException(
                                "Parameters to StringContainsFilter must not be null or empty");
                    }
                    return Pattern.quote(converter == null ? val : converter.convertStringLiteral(val).toString());
                });

        // If the match is simple, includes -any- or includes -none- we can just use a simple
        // regex of or'd values
        if ((matchType == MatchType.Regular && internalDisjunctive) ||
                (matchType == MatchType.Inverted && !internalDisjunctive)) {
            regex = valueStream.collect(Collectors.joining("|"));
        } else {
            // Note that internalDisjunctive is -always- false here.
            // If we need to match -all of- or -not one of- then we must use forward matching
            regex = valueStream.map(item -> "(?=.*" + item + ")")
                    .collect(Collectors.joining()) + ".*";
        }
        return regex;
    }
}
