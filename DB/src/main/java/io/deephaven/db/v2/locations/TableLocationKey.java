package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;

import java.util.Set;

/**
 * <p>Interface for opaque table location keys for use in {@link TableLocationProvider} implementations.
 * Note that implementations are generally only comparable to other implementations intended for use in the same
 * provider and discovery framework.
 *
 * <p>This interface also provides a mechanism for communicating <em>partition</em> information from a discovery
 * framework to the table engine. A partition of a table represents some sub-range of the overall available data, but
 * can always be thought of as a table in its own right. By representing partition membership as an ordered set of
 * key-value pairs with mutually-comparable values, we make it possible to:
 * <ol>
 *     <li>Totally order the set of partitions belonging to a table, and thus all rows of the table</li>
 *     <li>Refer to partitions via columns of the data, allowing vast savings in filtering efficiency for
 *     filters that only need evaluate one or more <em>partitioning columns</em></li>
 * </ol>
 *
 * <p>Generally, only {@link io.deephaven.db.v2.PartitionAwareSourceTable PartitionAwareSourceTable} and
 * {@link io.deephaven.db.v2.SourceTableMap SourceTableMap} are properly partition-aware.
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
    <PARTITION_VALUE_TYPE extends Comparable<PARTITION_VALUE_TYPE>> PARTITION_VALUE_TYPE getPartitionValue(@NotNull final String partitionKey);

    /**
     * Get the set of available partition keys.
     *
     * @return The set of available partition keys
     */
    Set<String> getPartitionKeys();

    /**
     * Get an {@link ImmutableTableLocationKey} that is equal to this.
     *
     * @return An immutable version of this key
     */
    ImmutableTableLocationKey makeImmutable();
}
