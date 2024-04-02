//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * Exception thrown when a requested partition value cannot be found because the partition key is unknown.
 */
public class UnknownPartitionKeyException extends UncheckedDeephavenException {

    public UnknownPartitionKeyException(@NotNull final String partitionKey) {
        super("Unknown partition key " + partitionKey);
    }

    public UnknownPartitionKeyException(@NotNull final String partitionKey,
            @NotNull final TableLocationKey locationKey) {
        super("Unknown partition key " + partitionKey + " for table location key " + locationKey);
    }
}
