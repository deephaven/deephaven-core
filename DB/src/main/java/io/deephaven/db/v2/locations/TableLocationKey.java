package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for opaque table location keys for use in {@link TableLocationProvider} implementations.
 * Note that implementations are generally only comparable to other implementations intended for use in the same
 * provider and discovery framework.
 */
public interface TableLocationKey extends Comparable<TableLocationKey>, NamedImplementation, LogOutputAppendable {

    /**
     * Lookup the value of one of the table partitions enclosing the location keyed by {@code this}.
     *
     * @param partitionKey The name of the partition
     * @param <PARTITION_VALUE_TYPE> The expected type of the partition value
     * @return The partition value
     * @throws UnknownPartitionKeyException If the partition cannot be found
     */
    <PARTITION_VALUE_TYPE> PARTITION_VALUE_TYPE getPartitionValue(@NotNull final String partitionKey);

    /**
     * Get an {@link ImmutableTableLocationKey} that is equal to this.
     *
     * @return An immutable version of this key
     */
    ImmutableTableLocationKey makeImmutable();
}
