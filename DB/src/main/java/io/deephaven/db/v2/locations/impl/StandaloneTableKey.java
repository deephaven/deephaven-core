package io.deephaven.db.v2.locations.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.db.v2.locations.ImmutableTableKey;
import io.deephaven.db.v2.locations.TableDataService;
import io.deephaven.db.v2.locations.TableKey;
import org.jetbrains.annotations.Nullable;

import javax.annotation.concurrent.Immutable;

/**
 * {@link TableKey} implementation for standalone tables that are created without a {@link TableDataService}.
 */
@Immutable
public final class StandaloneTableKey implements ImmutableTableKey {

    private static final String IMPLEMENTATION_NAME = StandaloneTableKey.class.getSimpleName();

    private static final TableKey INSTANCE = new StandaloneTableKey();

    public static TableKey getInstance() {
        return INSTANCE;
    }

    private StandaloneTableKey() {}

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(IMPLEMENTATION_NAME);
    }

    @Override
    public String toString() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        return other instanceof StandaloneTableKey;
    }
}
