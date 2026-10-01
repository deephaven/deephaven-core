//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned.kernel;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.impl.sort.SortedColumnPushdownManager;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ObjectChunkColumnSource;
import io.deephaven.test.types.ParallelTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;

import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.compareConsistentWithEquality;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.matchByOrdering;
import static io.deephaven.engine.table.impl.sources.regioned.kernel.BinarySearchKernelHelper.registerConsistentType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests the type set that decides whether a sorted binary search may answer a match by ordering alone, and the kernel
 * choice sorted pushdown makes from it.
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

    /**
     * Ordered by {@code value} alone but equal only when {@code tag} agrees too, so values that compare equal need not
     * be equal. A registered subclass claims otherwise, which lets a search reveal which kernel answered it.
     */
    private abstract static class TaggedValue implements Comparable<TaggedValue> {
        private final int value;
        private final String tag;

        private TaggedValue(final int value, @NotNull final String tag) {
            this.value = value;
            this.tag = tag;
        }

        @Override
        public int compareTo(@NotNull final TaggedValue other) {
            return Integer.compare(value, other.value);
        }

        @Override
        public boolean equals(final Object other) {
            if (other == null || other.getClass() != getClass()) {
                return false;
            }
            final TaggedValue that = (TaggedValue) other;
            return value == that.value && tag.equals(that.tag);
        }

        @Override
        public int hashCode() {
            return 31 * value + tag.hashCode();
        }

        @Override
        public String toString() {
            return value + tag;
        }
    }

    /** Registered as consistent with equality, which it is not. */
    private static final class RegisteredTaggedValue extends TaggedValue {
        private RegisteredTaggedValue(final int value, @NotNull final String tag) {
            super(value, tag);
        }
    }

    /** Never registered, so a search over it tests each row that compares equal for equality. */
    private static final class UnregisteredTaggedValue extends TaggedValue {
        private UnregisteredTaggedValue(final int value, @NotNull final String tag) {
            super(value, tag);
        }
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
     * A match is decided by ordering for every consistent type, and for {@link BigDecimal}, whose match filter matches
     * by compareTo, though its equals keeps it out of the consistent set.
     */
    @Test
    public void matchByOrderingAddsBigDecimal() {
        assertTrue(matchByOrdering(String.class));
        assertTrue(matchByOrdering(AnEnum.class));
        assertTrue(matchByOrdering(BigDecimal.class));
        assertFalse(compareConsistentWithEquality(BigDecimal.class));
        assertFalse(matchByOrdering(UnregisteredType.class));
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

    /**
     * Sorted pushdown chooses its Object match by {@link BinarySearchKernelHelper#matchByOrdering}. The middle row of
     * {@code [1a, 1b, 1a, 2a]} compares equal to {@code 1a} without being equal to it:
     * {@link ObjectColumnBinarySearchKernel#binarySearchMatchWithConsistentEquality}, which lets ordering alone decide
     * a match, returns it, and {@link ObjectColumnBinarySearchKernel#binarySearchMatchWithGeneralEquality}, which tests
     * each row for equality, does not. A registered type takes the first, as String, Instant and enums do, and so does
     * {@link BigDecimal}, whose match filter matches by compareTo; an unregistered type takes the second.
     */
    @Test
    public void sortedPushdownChoosesKernelByType() {
        registerConsistentType(RegisteredTaggedValue.class);

        assertMatch(RegisteredTaggedValue.class, new RegisteredTaggedValue[] {new RegisteredTaggedValue(1, "a"),
                new RegisteredTaggedValue(1, "b"), new RegisteredTaggedValue(1, "a"),
                new RegisteredTaggedValue(2, "a")},
                new RegisteredTaggedValue(1, "a"), 0, 1, 2);
        assertMatch(UnregisteredTaggedValue.class, new UnregisteredTaggedValue[] {new UnregisteredTaggedValue(1, "a"),
                new UnregisteredTaggedValue(1, "b"), new UnregisteredTaggedValue(1, "a"),
                new UnregisteredTaggedValue(2, "a")},
                new UnregisteredTaggedValue(1, "a"), 0, 2);
        assertMatch(BigDecimal.class,
                new BigDecimal[] {new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1.0"),
                        new BigDecimal("2.0")},
                new BigDecimal("1.0"), 0, 1, 2);

        // The types that are consistent with equality answer correctly through either match.
        assertMatch(String.class, new String[] {"a", "b", "b", "c"}, "b", 1, 2);
        assertMatch(Instant.class, new Instant[] {Instant.ofEpochSecond(1), Instant.ofEpochSecond(2),
                Instant.ofEpochSecond(2), Instant.ofEpochSecond(3)}, Instant.ofEpochSecond(2), 1, 2);
        assertMatch(AnEnum.class, new AnEnum[] {AnEnum.FIRST, AnEnum.SECOND, AnEnum.SECOND}, AnEnum.SECOND, 1, 2);
    }

    /**
     * Asserts that a sorted pushdown match for {@code toFind} over the ascending {@code data} returns exactly
     * {@code expectedKeys}.
     */
    private static <T> void assertMatch(
            @NotNull final Class<T> dataType,
            @NotNull final T[] data,
            @NotNull final T toFind,
            final long... expectedKeys) {
        @SuppressWarnings("unchecked")
        final ObjectChunkColumnSource<T> source =
                (ObjectChunkColumnSource<T>) ChunkColumnSource.make(ChunkType.Object, dataType);
        source.addChunk(ObjectChunk.chunkWrap(data));
        try (final RowSet selection = RowSetFactory.flat(data.length);
                final RowSet expected = RowSetFactory.fromKeys(expectedKeys);
                final RowSet matched = SortedColumnPushdownManager.binarySearchMatch(source, dataType, selection,
                        SortColumn.asc(ColumnName.of("test")), new Object[] {toFind}, false)) {
            assertEquals(dataType.getSimpleName(), expected, matched);
        }
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
