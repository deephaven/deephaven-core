package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;

/**
 * Table location key for simple (i.e. unpartitioned) tables.
 */
public final class SimpleTableLocationKey implements TableLocationKey {

    private static final TableLocationKey INSTANCE = new SimpleTableLocationKey();

    public static TableLocationKey getInstance() {
        return INSTANCE;
    }

    private SimpleTableLocationKey() {
    }


    @NotNull
    @Override
    public CharSequence getInternalPartition() {
        return NULL_PARTITION;
    }

    @NotNull
    @Override
    public CharSequence getColumnPartition() {
        return NULL_PARTITION;
    }

    @Override
    public String getImplementationName() {
        return "SimpleTableLocationKey";
    }
}
