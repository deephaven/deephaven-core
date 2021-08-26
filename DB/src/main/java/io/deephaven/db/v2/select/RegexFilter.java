/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import org.jetbrains.annotations.NotNull;

import java.util.regex.Pattern;

import static io.deephaven.db.v2.select.MatchFilter.CaseSensitivity;
import static io.deephaven.db.v2.select.MatchFilter.MatchType;

/**
 * A filter that matches using a regular expression.
 */
public class RegexFilter extends PatternFilter {

    private static final long serialVersionUID = 1L;

    public RegexFilter(MatchType matchType, String columnName, String value) {
        this(CaseSensitivity.MatchCase, matchType, columnName, value);
    }

    public RegexFilter(String columnName, String value) {
        this(MatchType.Regular, columnName, value);
    }

    public RegexFilter(CaseSensitivity sensitivity, MatchType matchType, @NotNull String columnName,
        String value) {
        super(sensitivity, matchType, columnName, value);
    }

    @Override
    protected Pattern compile(String value, int flags) {
        return Pattern.compile(value, flags | Pattern.DOTALL);
    }

    @Override
    protected boolean match(String columnValue) {
        return pattern.matcher(columnValue).matches();
    }

    public RegexFilter renameFilter(String newName) {
        return new RegexFilter(getCaseSensitivity(), getMatchType(), newName, value);
    }

    @Override
    public String toString() {
        return (invertMatch ? "!" : "") + columnName + " =~ /" + value + "/"
            + (caseInsensitive ? "i" : "");
    }

    @Override
    public RegexFilter copy() {
        return new RegexFilter(getCaseSensitivity(), getMatchType(), columnName, value);
    }
}
