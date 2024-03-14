//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

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
}
