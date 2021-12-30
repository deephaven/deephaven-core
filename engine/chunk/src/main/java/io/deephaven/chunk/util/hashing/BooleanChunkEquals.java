/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkEquals and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.chunk.util.hashing;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;

// region name
public class BooleanChunkEquals implements ChunkEquals {
    public static BooleanChunkEquals INSTANCE = new BooleanChunkEquals();
// endregion name

    public static boolean equalReduce(BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs) {
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

    public static int firstDifference(BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs) {
        int ii = 0;
        for (ii = 0; ii < lhs.size() && ii < rhs.size(); ++ii) {
            if (!eq(lhs.get(ii), rhs.get(ii))) {
                return ii;
            }
        }
        return ii;
    }

    private static void equal(BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, eq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static void equalNext(BooleanChunk<? extends Any> chunk, WritableBooleanChunk destination) {
        for (int ii = 0; ii < chunk.size() - 1; ++ii) {
            destination.set(ii, eq(chunk.get(ii), chunk.get(ii + 1)));
        }
        destination.setSize(chunk.size() - 1);
    }

    private static void equal(BooleanChunk<? extends Any> lhs, boolean rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, eq(lhs.get(ii), rhs));
        }
        destination.setSize(lhs.size());
    }

    public static void notEqual(BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, neq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    public static void notEqual(BooleanChunk<? extends Any> lhs, boolean rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, neq(lhs.get(ii), rhs));
        }
        destination.setSize(lhs.size());
    }

    private static void andEqual(BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, destination.get(ii) && eq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static void andNotEqual(BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destination) {
        for (int ii = 0; ii < lhs.size(); ++ii) {
            destination.set(ii, destination.get(ii) && neq(lhs.get(ii), rhs.get(ii)));
        }
        destination.setSize(lhs.size());
    }

    private static void andEqualNext(BooleanChunk<? extends Any> chunk, WritableBooleanChunk destination) {
        for (int ii = 0; ii < chunk.size() - 1; ++ii) {
            destination.set(ii, destination.get(ii) && eq(chunk.get(ii), chunk.get(ii + 1)));
        }
        destination.setSize(chunk.size() - 1);
    }

    private static void equalPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, BooleanChunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        final int pairCount = chunkPositionsToCheckForEquality.size() / 2;
        for (int ii = 0; ii < pairCount; ++ii) {
            final int firstPosition = chunkPositionsToCheckForEquality.get(ii * 2);
            final int secondPosition = chunkPositionsToCheckForEquality.get(ii * 2 + 1);
            final boolean equals = eq(valuesChunk.get(firstPosition), valuesChunk.get(secondPosition));
            destinations.set(ii, equals);
        }
        destinations.setSize(pairCount);
    }

    private static void andEqualPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, BooleanChunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
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

    private static void equalPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            final int lhsPosition = lhsPositions.get(ii);
            final int rhsPosition = rhsPositions.get(ii);
            final boolean equals = eq(lhs.get(lhsPosition), rhs.get(rhsPosition));
            destinations.set(ii, equals);
        }
        destinations.setSize(lhsPositions.size());
    }

    private static void andEqualPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destinations) {
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

    private static void equalLhsPermuted(IntChunk<ChunkPositions> lhsPositions, BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destinations) {
        for (int ii = 0; ii < lhsPositions.size(); ++ii) {
            final int lhsPosition = lhsPositions.get(ii);
            final boolean equals = eq(lhs.get(lhsPosition), rhs.get(ii));
            destinations.set(ii, equals);
        }
        destinations.setSize(lhsPositions.size());
    }

    private static void andEqualLhsPermuted(IntChunk<ChunkPositions> lhsPositions, BooleanChunk<? extends Any> lhs, BooleanChunk<? extends Any> rhs, WritableBooleanChunk destinations) {
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
        return equalReduce(lhs.asBooleanChunk(), rhs.asBooleanChunk());
    }

    @Override
    public void equal(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equal(lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    public static void equal(Chunk<? extends Any> lhs, boolean rhs, WritableBooleanChunk destination) {
        equal(lhs.asBooleanChunk(), rhs, destination);
    }

    @Override
    public void equalNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination) {
        equalNext(chunk.asBooleanChunk(), destination);
    }

    @Override
    public void andEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqual(lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    @Override
    public void andEqualNext(Chunk<? extends Any> chunk, WritableBooleanChunk destination) {
        andEqualNext(chunk.asBooleanChunk(), destination);
    }

    @Override
    public void equalPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equalPermuted(lhsPositions, rhsPositions, lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    @Override
    public void equalLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        equalLhsPermuted(lhsPositions, lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    @Override
    public void andEqualPermuted(IntChunk<ChunkPositions> lhsPositions, IntChunk<ChunkPositions> rhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqualPermuted(lhsPositions, rhsPositions, lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    @Override
    public void andEqualLhsPermuted(IntChunk<ChunkPositions> lhsPositions, Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andEqualLhsPermuted(lhsPositions, lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    @Override
    public void notEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        notEqual(lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    public static void notEqual(Chunk<? extends Any> lhs, boolean rhs, WritableBooleanChunk destination) {
        notEqual(lhs.asBooleanChunk(), rhs, destination);
    }

    @Override
    public void andNotEqual(Chunk<? extends Any> lhs, Chunk<? extends Any> rhs, WritableBooleanChunk destination) {
        andNotEqual(lhs.asBooleanChunk(), rhs.asBooleanChunk(), destination);
    }

    @Override
    public void equalPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        equalPairs(chunkPositionsToCheckForEquality, valuesChunk.asBooleanChunk(), destinations);
    }

    @Override
    public void andEqualPairs(IntChunk<ChunkPositions> chunkPositionsToCheckForEquality, Chunk<? extends Any> valuesChunk, WritableBooleanChunk destinations) {
        andEqualPairs(chunkPositionsToCheckForEquality, valuesChunk.asBooleanChunk(), destinations);
    }

    // region eq
    static private boolean eq(boolean lhs, boolean rhs) {
        return lhs == rhs;
    }
    // endregion eq

    // region neq
    static private boolean neq(boolean lhs, boolean rhs) {
        return !eq(lhs, rhs);
    }
    // endregion neq
}
