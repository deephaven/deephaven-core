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

    /**
     * Designates the expression as a barrier filter with the specified barrier name.
     * <p>
     * A barrier filter does not affect concurrency but imposes an ordering constraint on filters that respect the same
     * barrier. Filters specified before the barrier will be evaluated before it, and those specified after will be
     * evaluated after it.
     * <p>
     * Each barrier must be unique and declared by at most one filter.
     *
     * @param barrier the unique identifier for the barrier
     * @return a new instance of T with the barrier applied
     */
    T withBarrier(Object barrier);

    /**
     * Specifies that the expression should respect the ordering constraints of a barrier with the given name.
     * <p>
     * The expression will be executed in an order consistent with the barrier defined by the specified name. Filters
     * that define a barrier (using {@link #withBarrier(Object)}) will be executed either entirely before or entirely
     * after filters that respect that barrier.
     *
     * @param barrier the unique identifiers for all barriers to respect.
     * @return a new instance of T with the barrier ordering applied.
     */
    T respectsBarrier(Object... barrier);
}
