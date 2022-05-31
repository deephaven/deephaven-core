package io.deephaven.engine.table.impl;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Annotations library for the column-oriented hash tables used by join and aggregation operations.
 */
public class HashTableAnnotations {
    /**
     * We tag the empty state variable with this annotation, so we know what its name is in the source and destination.
     */
    @Retention(RetentionPolicy.RUNTIME)
    public @interface EmptyStateValue {
    }

    /**
     * We tag the state ColumnSource with this annotation, so we know what its name is in the source and destination.
     */
    @Retention(RetentionPolicy.RUNTIME)
    public @interface StateColumnSource {
    }

    /**
     * We tag the overflow state ColumnSource with this annotation, so we know what its name is in the source and
     * destination.
     */
    @Retention(RetentionPolicy.RUNTIME)
    public @interface OverflowStateColumnSource {
    }
}
