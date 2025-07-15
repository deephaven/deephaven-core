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
     * The serial wrapped filter/selectable is guaranteed to process exactly the set of rows as if any prior
     * filters/selectables were applied, but it will do so in a serial manner. If this is a filter, then the expression
     * will not evaluate additional rows or skip rows that future filters may eliminate. Take care when selecting
     * marking filters/selectables as serial, as the operation will not be able to take advantage of powerful
     * optimizations.
     * <p>
     * <ul>
     * <li>Concurrency impact: The expression will never be invoked concurrently with itself.</li>
     * <li>Intra-expression ordering impact: Rows are evaluated sequentially in row set order.</li>
     * <li>Inter-expression ordering impact: Acts as an absolute reordering barrier, ensuring that no parts of a
     * where/selectable clause are executed out of order relative to this serial wrapper.</li>
     * </ul>
     *
     * @return a new instance of T with serial concurrency control applied.
     */
    T withSerial();

    /**
     * Designates the filter/selectable as a barrier with the specified barrier object(s).
     * <p>
     * A barrier does not affect concurrency but imposes an ordering constraint for the filter/selectable that respect
     * the same barrier. When a filter/selectable is marked as respecting a barrier object, it indicates that the
     * respecting filter/selectable will be executed entirely after the filter/selectable declaring the barrier.
     * <p>
     * Each barrier must be unique and declared by at most one filter. Object {@link Object#equals(Object) equals} and
     * {@link Object#hashCode()} hashCode will be used to determine uniqueness and identification of barrier objects.
     *
     * @param barriers the unique barrier object identifiers
     * @return a new instance of T with the barriers applied
     */
    T withBarriers(Object... barriers);

    /**
     * Specifies that the filter/selectable should respect the ordering constraints of the given barriers.
     * <p>
     * Filters that define a barrier (using {@link #withBarriers(Object...)}) will be executed entirely before
     * filters/selectables that respect that barrier.
     * <p>
     * It is an error to respect a barrier that has not already been defined per the natural left to right ordering of
     * filters/selectables at the operation level. This is to minimize the risk of user error as the API is loosely
     * typed.
     * <p>
     * Object {@link Object#equals(Object) equals} and {@link Object#hashCode()} hashCode will be used to identify which
     * barriers are respected.
     *
     * @param barriers the unique barrier object identifiers to respect
     * @return a new instance of T with the respects barrier rule applied
     */
    T respectsBarriers(Object... barriers);
}
