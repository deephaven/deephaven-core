package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.time.DateTimeUtils;

import java.util.function.Supplier;

/**
 * A consistency monitor for use in the CompositeTableDataService.
 */
public class CompositeTableDataServiceConsistencyMonitor {
    /**
     * The default instance used by the CompositeTableDataServices.
     */
    static final FunctionConsistencyMonitor INSTANCE = new FunctionConsistencyMonitor();

    public static class ConsistentSupplier<T> extends FunctionConsistencyMonitor.ConsistentSupplier<T> {
        public ConsistentSupplier(Supplier<T> underlyingSupplier) {
            super(CompositeTableDataServiceConsistencyMonitor.INSTANCE, underlyingSupplier);
        }
    }

    private final static ConsistentSupplier<String> defaultCurrentDateNySupplier =
            new ConsistentSupplier<>(DateTimeUtils::currentDateNy);

    /**
     * Get the consistent value of currentDateNy() as defined by {@link DateTimeUtils#currentDateNy()}.
     *
     * @return the current date in the New York time zone.
     */
    public static String currentDateNy() {
        return defaultCurrentDateNySupplier.get();
    }

    /**
     * The same thing as {@link #currentDateNy()}, but with a different name so you can import it more easily and be
     * sure you are getting the right value.
     */
    public static String consistentDateNy() {
        return defaultCurrentDateNySupplier.get();
    }
}
