package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;

import javax.annotation.concurrent.Immutable;

/**
 * {@link TableKey} implementation for standalone tables that are created without a {@link TableDataService}.
 */
@Immutable
public final class StandaloneTableKey implements ImmutableTableKey {

    private static final String NAME = StandaloneTableKey.class.getSimpleName();

    private static final TableKey INSTANCE = new StandaloneTableKey();

    public static TableKey getInstance() {
        return INSTANCE;
    }

    private StandaloneTableKey() {
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
