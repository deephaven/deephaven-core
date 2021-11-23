package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.table.impl.locations.UnknownPartitionKeyException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Set;

/**
 * {@link TableLocationKey} implementation for unpartitioned standalone tables.
 */
public final class StandaloneTableLocationKey implements ImmutableTableLocationKey {

    private static final String NAME = StandaloneTableLocationKey.class.getSimpleName();

    private static final TableLocationKey INSTANCE = new StandaloneTableLocationKey();

    public static TableLocationKey getInstance() {
        return INSTANCE;
    }

    private StandaloneTableLocationKey() {}

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

    @Override
    public int compareTo(@NotNull final TableLocationKey other) {
        if (other instanceof StandaloneTableLocationKey) {
            return 0;
        }
        throw new ClassCastException("Cannot compare " + getClass() + " to " + other.getClass());
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(@Nullable Object other) {
        return other instanceof StandaloneTableLocationKey;
    }

    @Override
    public <PARTITION_VALUE_TYPE extends Comparable<PARTITION_VALUE_TYPE>> PARTITION_VALUE_TYPE getPartitionValue(
            @NotNull final String partitionKey) {
        throw new UnknownPartitionKeyException(partitionKey, this);
    }

    @Override
    public Set<String> getPartitionKeys() {
        return Collections.emptySet();
    }
}
