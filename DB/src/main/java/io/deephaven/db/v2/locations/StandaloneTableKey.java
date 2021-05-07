package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;

/**
 * Table location key for simple standalone tables.
 */
public final class StandaloneTableKey implements TableKey {

    private static final TableKey INSTANCE = new StandaloneTableKey();

    public static TableKey getInstance() {
        return INSTANCE;
    }

    private StandaloneTableKey() {
    }

    @NotNull
    @Override
    public String getNamespace() {
        return NULL_NAME;
    }

    @NotNull
    @Override
    public String getTableName() {
        return NULL_NAME;
    }

    @NotNull
    @Override
    public TableType getTableType() {
        return TableType.STANDALONE_SPLAYED;
    }

    @Override
    public String getImplementationName() {
        return "StandaloneTableKey";
    }
}
