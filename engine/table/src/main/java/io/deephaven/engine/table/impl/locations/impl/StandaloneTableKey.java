//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.table.impl.locations.ImmutableTableKey;
import io.deephaven.engine.table.impl.locations.TableDataService;
import io.deephaven.engine.table.impl.locations.TableKey;
import org.jetbrains.annotations.Nullable;

import javax.annotation.concurrent.Immutable;

/**
 * {@link TableKey} implementation for standalone tables that are created without a {@link TableDataService}.
 */
@Immutable
public final class StandaloneTableKey implements ImmutableTableKey {

    private static final TableKey INSTANCE = new StandaloneTableKey();

    public static TableKey getInstance() {
        return INSTANCE;
    }

    private StandaloneTableKey() {}

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(getImplementationName());
    }

    @Override
    public String toString() {
        return getImplementationName();
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
