package io.deephaven.db.v2.locations.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.db.v2.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;
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
        return logOutput.append(getImplementationName()).append("[partitions=").append(PartitionsFormatter.INSTANCE, partitions).append(']');
    }

    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof SimpleTableLocationKey) {
            return PartitionsComparator.INSTANCE.compare(partitions, ((SimpleTableLocationKey) other).partitions);
        }
        throw new ClassCastException("Cannot compare " + getClass() + " to " + other.getClass());
    }

    @Override
    public int hashCode() {
        return partitions.hashCode();
    }

    @Override
    public boolean equals(final Object other) {
        return other == this || (other instanceof SimpleTableLocationKey && partitions.equals(((SimpleTableLocationKey) other).partitions));
    }
}
