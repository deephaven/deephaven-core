//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.test.types.ParallelTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.compareConsistentWithEquality;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.registerConsistentType;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests the type set that decides whether a sorted binary search may answer a match by ordering alone.
 *
 * <p>
 * Registration is process-wide and cannot be undone, so every type registered here is declared in this file and named
 * for it. Nothing else can be looking them up, which is what makes the leftover state harmless.
 */
@Category(ParallelTest.class)
public class BinarySearchKernelHelperTest {

    /**
     * Consistent with equals, but not a type the engine could know about. Registration cannot be undone and the test
     * methods may run in any order, so no two tests share one of these -- each registers its own.
     */
    private static final class RegisteredOnceType implements Comparable<RegisteredOnceType> {
        @Override
        public int compareTo(@NotNull final RegisteredOnceType other) {
            return 0;
        }
    }

    /** @see RegisteredOnceType */
    private static final class RegisteredTwiceType implements Comparable<RegisteredTwiceType> {
        @Override
        public int compareTo(@NotNull final RegisteredTwiceType other) {
            return 0;
        }
    }

    /** Never registered, so it stands for every type an installation has not vouched for. */
    private static final class UnregisteredType implements Comparable<UnregisteredType> {
        @Override
        public int compareTo(@NotNull final UnregisteredType other) {
            return 0;
        }
    }

    /** Not orderable at all, so registering it is a mistake the call can catch. */
    private static final class NotComparableType {
    }

    private enum AnEnum {
        FIRST, SECOND
    }

    @Test
    public void seededTypesAreConsistent() {
        assertTrue(compareConsistentWithEquality(String.class));
        assertTrue(compareConsistentWithEquality(BigInteger.class));
        assertTrue(compareConsistentWithEquality(Boolean.class));
        assertTrue(compareConsistentWithEquality(Instant.class));
        // Ordered by ordinal, compared by identity.
        assertTrue(compareConsistentWithEquality(AnEnum.class));
        // The counterexample the distinction exists for.
        assertFalse(compareConsistentWithEquality(BigDecimal.class));
    }

    /**
     * An unknown type is answered {@code false}, which costs the ordering-only shortcut but not correctness, and stays
     * that way until it is registered.
     */
    @Test
    public void registrationAddsAType() {
        assertFalse(compareConsistentWithEquality(RegisteredOnceType.class));

        registerConsistentType(RegisteredOnceType.class);
        assertTrue(compareConsistentWithEquality(RegisteredOnceType.class));

        // Registering one type says nothing about any other.
        assertFalse(compareConsistentWithEquality(UnregisteredType.class));
        assertFalse(compareConsistentWithEquality(BigDecimal.class));
        // And the seeded types survive the set being rebuilt.
        assertTrue(compareConsistentWithEquality(String.class));
    }

    /** Registering twice is not an error, and the second call leaves the answer alone. */
    @Test
    public void registrationIsIdempotent() {
        registerConsistentType(RegisteredTwiceType.class);
        registerConsistentType(RegisteredTwiceType.class);
        assertTrue(compareConsistentWithEquality(RegisteredTwiceType.class));
    }

    /**
     * A type that cannot be ordered could never reach a binary search, so registering it is rejected outright rather
     * than left to fail as a {@link ClassCastException} deep in a comparison.
     */
    @Test
    public void registeringNonComparableIsRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> registerConsistentType(NotComparableType.class));
        assertFalse(compareConsistentWithEquality(NotComparableType.class));
    }
}
