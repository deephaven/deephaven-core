//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.util.annotations.VisibleForTesting;

/**
 * Internal library with utility methods for converting time data between Deephaven and Parquet.
 *
 * @implNote Some duplication with DateTimeUtils is intentional to minimize dependency.
 */
@VisibleForTesting
public class ParquetMaterializerUtils {

    /**
     * One nanosecond in nanoseconds.
     */
    static final long NANO = 1;

    /**
     * One microsecond in nanoseconds.
     */
    static final long MICRO = 1_000;

    /**
     * One millisecond in nanoseconds.
     */
    static final long MILLI = 1_000_000;

    /**
     * Maximum time in microseconds that can be converted to an instant without overflow.
     */
    @VisibleForTesting
    public static final long MAX_CONVERTIBLE_MICROS = Long.MAX_VALUE / 1_000L;

    /**
     * Maximum time in milliseconds that can be converted to an instant without overflow.
     */
    @VisibleForTesting
    public static final long MAX_CONVERTIBLE_MILLIS = Long.MAX_VALUE / 1_000_000L;
}
