//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.gui.table;

public enum QuickFilterMode {
    // @formatter:off
    NORMAL("Normal"),
    NUMERIC("Numeric Only"),
    MULTI("Multi-column"),
    AND("AND (String Only)"),
    OR("OR"),
    WILDCARD("Wildcard"),
    REGEX("Regex");
    // @formatter:on

    private final String displayName;

    QuickFilterMode(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return displayName;
    }
}
