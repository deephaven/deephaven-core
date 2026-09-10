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
import java.util.HashSet;
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
     * Types documented to have a natural ordering consistent with equals, seeded with those the engine knows and
     * extended by {@link #registerConsistentType(Class)}. Boxed primitives are absent deliberately: they never reach
     * the Object kernels, since the sorted pushdown dispatches them to their primitive kernel.
     *
     * <p>
     * Replaced wholesale rather than mutated, so a read is an ordinary lookup in an immutable set and every search that
     * consults it pays nothing for the fact that it can change. Writes take the cost instead, which is where it
     * belongs: registration happens a handful of times at startup, while this is read once per pushdown.
     */
    private static volatile Set<Class<?>> consistentTypes = Set.of(
            String.class,
            BigInteger.class,
            Boolean.class,
            Instant.class,
            LocalDate.class,
            LocalTime.class,
            LocalDateTime.class,
            Duration.class);

    /**
     * Registers {@code dataType} as ordering consistently with equality, letting a sorted binary search answer a match
     * over a column of that type by ordering alone. See {@link #compareConsistentWithEquality(Class)} for what that
     * decides.
     *
     * <p>
     * <em>The caller warrants the property; nothing here can verify it.</em> Register a type for which
     * {@code compare(a, b) == 0} does not imply {@code eq(a, b)} and the search will claim rows that the filter it
     * stands in for would reject. The result is declared exact, so nothing downstream re-checks it, and the query
     * returns wrong rows with no error -- exactly the failure {@link java.math.BigDecimal} produces.
     *
     * <p>
     * Registration is additive and idempotent, and a type cannot be withdrawn. Register during startup, before the type
     * is queried: a search already under way keeps the set it started with, so registering alongside a running query
     * decides nothing about which path that query takes.
     *
     * @param dataType the column data type to register
     * @throws IllegalArgumentException if {@code dataType} is not {@link Comparable}, since
     *         {@link io.deephaven.util.compare.ObjectComparisons#compare(Object, Object)} could not order it at all
     */
    public static synchronized void registerConsistentType(@NotNull final Class<?> dataType) {
        if (!Comparable.class.isAssignableFrom(dataType)) {
            throw new IllegalArgumentException("Cannot register " + dataType.getName()
                    + " as comparing consistently with equality; it is not Comparable, so it cannot be ordered");
        }
        if (consistentTypes.contains(dataType)) {
            return;
        }
        // Publish a new immutable set rather than mutating the live one, which unsynchronized readers are inside.
        final Set<Class<?>> extended = new HashSet<>(consistentTypes);
        extended.add(dataType);
        consistentTypes = Set.copyOf(extended);
    }

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
     * Only this stronger both-ways guarantee is checked, and only where documented, since {@link java.math.BigDecimal}
     * is a common counterexample. A {@code false} answer still assumes the weaker
     * {@code eq(a, b) implies compare(a, b) == 0}, which {@link Comparable} recommends and without which a type is
     * unusable in any sorted context. An enum qualifies because its ordering is by ordinal and its equality is
     * identity.
     *
     * <p>
     * The engine's own types are answered here; a type it does not know is answered {@code false} until
     * {@link #registerConsistentType(Class)} says otherwise, so an unrecognized type costs speed rather than
     * correctness.
     *
     * @param dataType the column's data type
     * @return {@code true} if a search by ordering alone decides a match for this type
     */
    public static boolean compareConsistentWithEquality(@NotNull final Class<?> dataType) {
        return consistentTypes.contains(dataType) || dataType.isEnum();
    }
}
