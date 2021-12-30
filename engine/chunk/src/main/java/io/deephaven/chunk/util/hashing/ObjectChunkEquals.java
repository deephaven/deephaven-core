/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkEquals and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import java.util.Objects;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

// region name
public class ObjectChunkEquals implements ChunkEquals {
    public static ObjectChunkEquals INSTANCE = new ObjectChunkEquals();
// endregion name

    public static boolean equalReduce(ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (int ii = 0; ii < lhs.size(); ++ii) {
            if (!eq(lhs.get(ii), rhs.get(ii))) {
                return false;
            }
        }
        return true;
    }

    public static int firstDifference(ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs) {
        int ii = 0;
        for (ii = 0; ii < lhs.size() && ii < rhs.size(); ++ii) {
            if (!eq(lhs.get(ii), rhs.get(ii))) {
                return ii;
            }
        }
        return ii;
    }

    private static void equal(ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, eq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static void equalNext(ObjectChunk<Object, ? extends Any> chunk, WritableBooleanChunk destination) {
        for (int ii = 0; ii < chunk.size() - 1; ++ii) {
            destination.set(ii, eq(chunk.get(ii), chunk.get(ii + 1)));
        }
        destination.setSize(chunk.size() - 1);
    }

    private static void equal(ObjectChunk<Object, ? extends Any> lhs, Object rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, eq(lhs.get(ii), rhs));
        }
        destination.setSize(lhs.size());
    }

    public static void notEqual(ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, neq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    public static void notEqual(ObjectChunk<Object, ? extends Any> lhs, Object rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, neq(lhs.get(ii), rhs));
        }
        destination.setSize(lhs.size());
    }

    private static void andEqual(ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, destination.get(ii) && eq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static void andNotEqual(ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, destination.get(ii) && neq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static void andEqualNext(ObjectChunk<Object, ? extends Any> chunk, WritableBooleanChunk destination) {
        for (int ii = 0; ii < chunk.size() - 1; ++ii) {
            destination.set(ii, destination.get(ii) && eq(chunk.get(ii), chunk.get(ii + 1)));
        }
        destination.setSize(chunk.size() - 1);
    }

    private static void equalPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, ObjectChunk<Object, ? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        final int pairCount = chunkPositionsToCheckForEquality.size() / 2;
        for (int ii = 0; ii < pairCount; ++ii) {
            final int firstPosition = chunkPositionsToCheckForEquality.get(ii * 2);
            final int secondPosition = chunkPositionsToCheckForEquality.get(ii * 2 + 1);
            final boolean equals = eq(valuesChunk.get(firstPosition), valuesChunk.get(secondPosition));
            destinations.set(ii, equals);
        }
        destinations.setSize(pairCount);
    }

    private static void andEqualPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, ObjectChunk<Object, ? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        final int pairCount = chunkPositionsToCheckForEquality.size() / 2;
        for (int ii = 0; ii < pairCount; ++ii) {
            if (destinations.get(ii)) {
                final int firstPosition = chunkPositionsToCheckForEquality.get(ii * 2);
                final int secondPosition = chunkPositionsToCheckForEquality.get(ii * 2 + 1);
                final boolean equals = eq(valuesChunk.get(firstPosition), valuesChunk.get(secondPosition));
                destinations.set(ii, equals);
            }
        }
    }

    private static void equalPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            final int lhsPosition = lhsPositions.get(ii);
            final int rhsPosition = rhsPositions.get(ii);
            final boolean equals = eq(lhs.get(lhsPosition), rhs.get(rhsPosition));
            destinations.set(ii, equals);
        }
        destinations.setSize(lhsPositions.size());
    }

    private static void andEqualPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            if (destinations.get(ii)) {
                final int lhsPosition = lhsPositions.get(ii);
                final int rhsPosition = rhsPositions.get(ii);
                final boolean equals = eq(lhs.get(lhsPosition), rhs.get(rhsPosition));
                destinations.set(ii, equals);
            }
        }
        destinations.setSize(lhsPositions.size());
    }

    private static void equalLhsPermuted(IntChunk<ChunkPositions> lhsPositions, ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            final int lhsPosition = lhsPositions.get(ii);
            final boolean equals = eq(lhs.get(lhsPosition), rhs.get(ii));
            destinations.set(ii, equals);
        }
        destinations.setSize(lhsPositions.size());
    }

    private static void andEqualLhsPermuted(IntChunk<ChunkPositions> lhsPositions, ObjectChunk<Object, ? extends Any> lhs, ObjectChunk<Object, ? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            if (destinations.get(ii)) {
                final int lhsPosition = lhsPositions.get(ii);
                final boolean equals = eq(lhs.get(lhsPosition), rhs.get(ii));
                destinations.set(ii, equals);
            }
        }
        destinations.setSize(lhsPositions.size());
    }

    @Override
    public boolean equalReduce(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs) {
        return equalReduce(lhs.asObjectChunk(), rhs.asObjectChunk());
    }

    @Override
    public void equal(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equal(lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    public static void equal(Chunk<? extends Any> lhs, Object rhs, WritableBooleanChunk destination) {
        equal(lhs.asObjectChunk(), rhs, destination);
    }

    @Override
    public void equalNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination) {
        equalNext(chunk.asObjectChunk(), destination);
    }

    @Override
    public void andEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqual(lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andEqualNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination) {
        andEqualNext(chunk.asObjectChunk(), destination);
    }

    @Override
    public void equalPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equalPermuted(lhsPositions, rhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void equalLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equalLhsPermuted(lhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andEqualPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqualPermuted(lhsPositions, rhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void andEqualLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqualLhsPermuted(lhsPositions, lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void notEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        notEqual(lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    public static void notEqual(Chunk<? extends Any> lhs, Object rhs, WritableBooleanChunk destination) {
        notEqual(lhs.asObjectChunk(), rhs, destination);
    }

    @Override
    public void andNotEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andNotEqual(lhs.asObjectChunk(), rhs.asObjectChunk(), destination);
    }

    @Override
    public void equalPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        equalPairs(chunkPositionsToCheckForEquality, valuesChunk.asObjectChunk(), destinations);
    }

    @Override
    public void andEqualPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        andEqualPairs(chunkPositionsToCheckForEquality, valuesChunk.asObjectChunk(), destinations);
    }

    // region eq
    static private boolean eq(Object lhs, Object rhs) {
        return Objects.equals(lhs, rhs);
    }
    // endregion eq

    // region neq
    static private boolean neq(Object lhs, Object rhs) {
        return !eq(lhs, rhs);
    }
    // endregion neq
}
