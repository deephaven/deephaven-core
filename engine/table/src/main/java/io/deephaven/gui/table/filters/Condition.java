//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.gui.table.filters;

/**
 * The set of Conditions that can be used with various filters.
 */
public enum Condition {
    // @formatter:off
    EQUALS("equals", true),
    NOT_EQUALS("not equals", false),

    // Strings
    INCLUDES("includes", true),
    NOT_INCLUDES("not includes", false),
    EQUALS_MATCH_CASE("equals (casesen)", true),
    NOT_EQUALS_MATCH_CASE("not equals (casesen)", false),
    INCLUDES_MATCH_CASE("includes (casesen)", true),
    NOT_INCLUDES_MATCH_CASE("not includes (casesen)", false),

    // Numbers and Dates
    LESS_THAN("less than", false) {
        @Override
        public Condition mirror() {
            return Condition.GREATER_THAN;
        }
    },
    GREATER_THAN("greater than", false) {
        @Override
        public Condition mirror() {
            return Condition.LESS_THAN;
        }
    },
    LESS_THAN_OR_EQUAL("less than or equal to", false) {
        @Override
        public Condition mirror() {
            return Condition.GREATER_THAN_OR_EQUAL;
        }
    },
    GREATER_THAN_OR_EQUAL("greater than or equal to", false) {
        @Override
        public Condition mirror() {
            return Condition.LESS_THAN_OR_EQUAL;
        }
    },

    // Numbers
    EQUALS_ABS("equals (abs)", true),
    NOT_EQUALS_ABS("not equals (abs)", false),
    LESS_THAN_ABS("less than (abs)", false) {
        @Override
        public Condition mirror() {
            return Condition.GREATER_THAN_ABS;
        }
    },
    GREATER_THAN_ABS("greater than (abs)", false) {
        @Override
        public Condition mirror() {
            return Condition.LESS_THAN_ABS;
        }
    },

    // Lists
    INCLUDED_IN("included in list", true),
    INCLUDED_IN_MATCH_CASE("included in list (casesen)", true),
    NOT_INCLUDED_IN("not included in list", false),
    NOT_INCLUDED_IN_MATCH_CASE("not included in list (casesen)", false);
    // @formatter:on

    public final String description;
    public final boolean defaultOr;

    Condition(String description, boolean defaultOr) {
        this.description = description;
        this.defaultOr = defaultOr;
    }

    public Condition mirror() {
        return this;
    }
}
