//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Set;

/**
 * Helper methods for binary search kernels.
 */
public class BinarySearchKernelHelper {
    /**
     * Private constructor to prevent instantiation.
     */
    private BinarySearchKernelHelper() {}

    /**
     * Helper to convert array index to insertion index (and back again).
     */
    static long insertionPoint(final long index) {
        return -index - 1;
    }

    /**
     * Types documented to have a natural ordering consistent with equals. Boxed primitives are absent deliberately:
     * they never reach the Object kernels, since the sorted pushdown dispatches them to their primitive kernel.
     */
    private static final Set<Class<?>> COMPARE_CONSISTENT_TYPES = Set.of(
            String.class,
            BigInteger.class,
            Boolean.class,
            Instant.class,
            LocalDate.class,
            LocalTime.class,
            LocalDateTime.class,
            Duration.class);

    /**
     * Whether {@code dataType} compares consistently with equality, meaning
     * {@code ObjectComparisons.compare(a, b) == 0} exactly when {@code ObjectComparisons.eq(a, b)}, for every pair of
     * values.
     *
     * <p>
     * This decides how a sorted binary search may answer a match. The search navigates by
     * {@link io.deephaven.util.compare.ObjectComparisons#compare(Object, Object)}, which is
     * {@link Comparable#compareTo(Object)}, while a match is decided by
     * {@link io.deephaven.util.compare.ObjectComparisons#eq(Object, Object)}, which is
     * {@link java.util.Objects#equals(Object, Object)} -- the same relation the chunk filter uses. When the two agree,
     * the ordering-equal run the search locates is exactly the set of matching rows and the search can answer the match
     * outright. When they disagree -- {@link java.math.BigDecimal} at differing scales, for one -- that run is only a
     * superset, and the matches have to be picked out of it by equality.
     *
     * <p>
     * Answering {@code false} is always safe, so a type is listed only where the guarantee is documented. An enum
     * qualifies because its ordering is by ordinal and its equality is identity.
     *
     * @param dataType the column's data type
     * @return {@code true} if a search by ordering alone decides a match for this type
     */
    public static boolean compareConsistentWithEquality(@NotNull final Class<?> dataType) {
        return COMPARE_CONSISTENT_TYPES.contains(dataType) || dataType.isEnum();
    }
}
