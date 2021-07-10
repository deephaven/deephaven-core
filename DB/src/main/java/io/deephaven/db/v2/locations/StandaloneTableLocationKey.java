package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;

/**
 * {@link TableLocationKey} implementation for unpartitioned standalone tables.
 */
public final class StandaloneTableLocationKey implements ImmutableTableLocationKey {

    private static final String NAME = StandaloneTableLocationKey.class.getSimpleName();

    private static final TableLocationKey INSTANCE = new StandaloneTableLocationKey();

    public static TableLocationKey getInstance() {
        return INSTANCE;
    }

    private StandaloneTableLocationKey() {
    }

    @Override
    public String getImplementationName() {
        return NAME;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(NAME);
    }

    @Override
    public String toString() {
        return NAME;
    }
}
