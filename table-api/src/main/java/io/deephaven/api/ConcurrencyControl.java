//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api;

import io.deephaven.api.filter.Filter;

/**
 * Provides concurrency control operations for expressions such as {@link Filter filters} and {@link Selectable
 * selectables}.
 * <p>
 * This interface allows the application of different concurrency strategies to an expression. Through the provided
 * methods, clients can enforce serial execution, apply barrier constraints, and specify ordering of operations relative
 * to barriers. This facilitates both intra- and inter-expression synchronization, ensuring that expressions can be
 * executed without risking concurrent modifications or out-of-order processing when interacting with stateful parts of
 * custom user code.
 *
 * @param <T> the type of the expression to which concurrency control is applied.
 */
public interface ConcurrencyControl<T> {

    /**
     * Applies serial concurrency control to the expression.
     * <p>
     * The serial filter is guaranteed to filter exactly the set of rows that passed any prior filters, without
     * evaluating additional rows or skipping rows that future filters may eliminate. Take care when selecting a serial
     * filter, as subsequent filters cannot apply optimizations like predicate push down.</li>
     * <p>
     * <ul>
     * <li>Concurrency impact: The expression will never be invoked concurrently with itself.</li>
     * <li>Intra-expression ordering impact: Rows are evaluated sequentially in row set order.</li>
     * <li>Inter-expression ordering impact: Acts as an absolute reordering barrier, ensuring that no parts of a where
     * clause are executed out of order relative to this serial filter.</li>
     * </ul>
     *
     * @return a new instance of T with serial concurrency control applied.
     */
    T withSerial();
}
