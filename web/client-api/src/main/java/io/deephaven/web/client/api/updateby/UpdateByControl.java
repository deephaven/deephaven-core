//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;

/**
 * Optional advice to give the server on how to manage the work of maintaining the updateBy operation. Any option left
 * unset uses the server's default.
 */
@JsType(namespace = "dh.updateby")
public class UpdateByControl {
    /**
     * Whether redirections are used for output sources instead of sparse array sources.
     */
    @JsNullable
    public Boolean useRedirection;

    /**
     * The maximum chunk capacity.
     */
    @JsNullable
    public Double chunkCapacity;

    /**
     * The maximum fractional memory overhead allowable for sparse redirections as a fraction, for example, 1.1 allows
     * 10% overhead. Values less than zero disable overhead checking, and result in always using the sparse structure. A
     * value of zero results in never using the sparse structure.
     */
    @JsNullable
    public Double maxStaticSparseMemoryOverhead;

    /**
     * The initial hash table size.
     */
    @JsNullable
    public Double initialHashTableSize;

    /**
     * The maximum load factor for the hash table.
     */
    @JsNullable
    public Double maximumLoadFactor;

    /**
     * The target load factor for the hash table.
     */
    @JsNullable
    public Double targetLoadFactor;

    /**
     * The precision and rounding mode for BigDecimal arithmetic in these operations:
     * <ul>
     * <li>{@link UpdateByCumulativeSum}, {@link UpdateByCumulativeProduct}, {@link UpdateByRollingSum} and
     * {@link UpdateByRollingProduct} on BigDecimal columns</li>
     * <li>{@link UpdateByRollingAvg} and {@link UpdateByRollingStd} on BigDecimal and BigInteger columns</li>
     * <li>{@link UpdateByRollingWAvg} when the value or weight column is BigDecimal or BigInteger</li>
     * </ul>
     * Defaults to Java's {@code MathContext.DECIMAL64}, 16 digits with {@code HALF_EVEN} rounding. The server's
     * {@code UpdateByControl.mathContext} system property can change the default.
     */
    @JsNullable
    public MathContext mathContext;
}
