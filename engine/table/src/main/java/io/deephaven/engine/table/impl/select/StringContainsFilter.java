/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.select;

import io.deephaven.engine.util.string.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.deephaven.engine.table.impl.select.MatchFilter.CaseSensitivity;
import static io.deephaven.engine.table.impl.select.MatchFilter.MatchType;

/**
 * WhereFilter that implements String.contains(valueToMatch), for use in QuickFilter so that we can bypass compilation
 * on small tables.
 */
public class StringContainsFilter extends PatternFilter {
    private static final long serialVersionUID = 2L;

    private final boolean internalDisjunctive;
    private final String[] values;

    public StringContainsFilter(MatchType matchType, String columnName, String... values) {
        this(CaseSensitivity.MatchCase, matchType, columnName, values);
    }

    public StringContainsFilter(String columnName, String... values) {
        this(MatchType.Regular, columnName, values);
    }

    public StringContainsFilter(CaseSensitivity sensitivity, MatchType matchType, @NotNull String columnName,
            String... values) {
        this(sensitivity, matchType, columnName, true, false, values);
    }

    public StringContainsFilter(CaseSensitivity sensitivity, MatchType matchType, @NotNull String columnName,
            boolean internalDisjuntive, boolean removeQuotes, String... values) {
        super(sensitivity, matchType, columnName,
                constructRegex(values, matchType, internalDisjuntive, removeQuotes, columnName));
        this.internalDisjunctive = internalDisjuntive;
        this.values = values;
    }

    private static String constructRegex(String[] values, MatchType matchType, boolean internalDisjunctive,
            boolean removeQuotes, String columnName) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException(
                    "StringContainsFilter must be created with at least one value parameter");
        }

        final MatchFilter.ColumnTypeConvertor converter =
                removeQuotes ? MatchFilter.ColumnTypeConvertorFactory.getConvertor(String.class, columnName) : null;
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

    @Override
    protected Pattern compile(String regex, int flags) {
        return Pattern.compile(regex, flags);
    }

    @Override
    protected boolean match(String columnValue) {
        return pattern.matcher(columnValue).find();
    }

    public StringContainsFilter renameFilter(String newName) {
        return new StringContainsFilter(getCaseSensitivity(), getMatchType(), newName, values);
    }

    @Override
    public String toString() {
        return (invertMatch ? "!" : "") + columnName + ".contains"
                + ((values.length == 1) ? "" : internalDisjunctive ? "Any" : "All")
                + (caseInsensitive ? "IgnoreCase" : "") + "(\"" + value + "\")";
    }

    @Override
    public StringContainsFilter copy() {
        return new StringContainsFilter(getCaseSensitivity(), getMatchType(), columnName, values);
    }
}
