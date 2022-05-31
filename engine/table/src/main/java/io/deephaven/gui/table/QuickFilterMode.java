package io.deephaven.gui.table;

public enum QuickFilterMode {
    NORMAL("Normal"), NUMERIC("Numeric Only"), MULTI("Multi-column"), AND("AND (String Only)"), OR("OR"), WILDCARD(
            "Wildcard"), REGEX("Regex");

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
