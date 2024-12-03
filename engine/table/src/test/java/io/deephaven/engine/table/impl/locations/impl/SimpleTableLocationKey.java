//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.log.LogOutput;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Simple {@link PartitionedTableLocationKey} implementation for unit tests.
 */
public final class SimpleTableLocationKey extends PartitionedTableLocationKey {

    private static final String IMPLEMENTATION_NAME = SimpleTableLocationKey.class.getSimpleName();

    public SimpleTableLocationKey(@Nullable final Map<String, Comparable<?>> partitions) {
        super(partitions);
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append(getImplementationName()).append("[partitions=")
                .append(PartitionsFormatter.INSTANCE, partitions).append(']');
    }
}
