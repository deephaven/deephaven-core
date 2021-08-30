/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.gui.table.filters;

/**
 * The set of Conditions that can be used with various {@link FilterData} types. Note that not all
 * {@link FilterData} classes support all types.
 */
public enum Condition {
    EQUALS("equals", true), NOT_EQUALS("not equals", false),

    // Strings
    INCLUDES("includes", true), NOT_INCLUDES("not includes", false), EQUALS_MATCH_CASE(
        "equals (casesen)",
        true), NOT_EQUALS_MATCH_CASE("not equals (casesen)", false), INCLUDES_MATCH_CASE(
            "includes (casesen)", true), NOT_INCLUDES_MATCH_CASE("not includes (casesen)", false),

    // Numbers and Dates
    LESS_THAN("less than", false), GREATER_THAN("greater than", false), LESS_THAN_OR_EQUAL(
        "less than or equal to", false), GREATER_THAN_OR_EQUAL("greater than or equal to", false),

    // Numbers
    EQUALS_ABS("equals (abs)", true), NOT_EQUALS_ABS("not equals (abs)", false), LESS_THAN_ABS(
        "less than (abs)", false), GREATER_THAN_ABS("greater than (abs)", false),

    // Lists
    INCLUDED_IN("included in list", true), INCLUDED_IN_MATCH_CASE("included in list (casesen)",
        true), NOT_INCLUDED_IN("not included in list",
            false), NOT_INCLUDED_IN_MATCH_CASE("not included in list (casesen)", false);

    public final String description;
    public final boolean defaultOr;

    Condition(String description, boolean defaultOr) {
        this.description = description;
        this.defaultOr = defaultOr;
    }
}
