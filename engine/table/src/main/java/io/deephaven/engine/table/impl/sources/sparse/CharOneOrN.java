//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.SoftRecycler;
import io.deephaven.util.annotations.TestUseOnly;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.stream.IntStream;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;

public final class CharOneOrN {
    private static final int REFERENCE_SIZE = 8;
    private static final int ELEMENT_SIZE = Character.BYTES;

    private static final int ONE_UNINITIALIZED = -1;
    private static final int HAVE_MORE_THAN_ONE = -2;

    private CharOneOrN() {
        throw new IllegalStateException("CharOneOrN class should never be constructed.");
    }

    // region Block0
    static public final class Block0 {
        int oneIndex = ONE_UNINITIALIZED;
        Block1 oneValue;
        Block1 [] array;

        public void ensureIndex(int idx, SoftRecycler<Block1[]> recycler) {
            if (oneIndex == idx || oneIndex == HAVE_MORE_THAN_ONE) {
                return;
            }
            if (oneIndex == ONE_UNINITIALIZED) {
                oneIndex = idx;
            } else {
                if (recycler == null) {
                    array = new Block1[BLOCK0_SIZE];
                } else {
                    array = recycler.borrowItem();
                }
                array[oneIndex] = oneValue;
                oneIndex = HAVE_MORE_THAN_ONE;
                oneValue = null;
            }
        }

        /**
         * Called by the {@link io.deephaven.engine.table.impl.sources.CharacterSparseArraySource} at the end of a cycle
         * when blocks have been completely eliminated from the output rowset.  The input is not a row key in our address
         * space, but rather an index for the block to clear (i.e. row key >> {@link SparseConstants#BLOCK2_SHIFT}).
         */
         public void clearLowestLevelBlocks(final RowSet blocksToClear,
                                           final SoftRecycler<char[]> recycler) {
            blocksToClear.forAllRowKeys(idx -> clearOneLowestLevelBlock(idx, recycler));
        }

        /**
         * Called by the {@link io.deephaven.engine.table.impl.sources.CharacterSparseArraySource} at the end of a cycle
         * when blocks have been completely eliminated from the output rowset.  The input is not a row key in our address
         * space, but rather an index for the block2 to clear (i.e. row key >> {@link SparseConstants#BLOCK1_SHIFT}).
         */
         public void clearBlocks2(final RowSet blocks2ToClear,
                                           final SoftRecycler<char[][]> recycler2) {
            blocks2ToClear.forAllRowKeys(idx -> clearOneBlock_2(idx, recycler2));
        }

        /**
         * Called by the {@link io.deephaven.engine.table.impl.sources.CharacterSparseArraySource} at the end of a cycle
         * when blocks have been completely eliminated from the output rowset.  The input is not a row key in our address
         * space, but rather an index for the block2 to clear (i.e. row key >> {@link SparseConstants#BLOCK0_SHIFT}).
         */
         public void clearBlocks1(final RowSet blocks1ToClear,
                                           final SoftRecycler<Block2[]> recycler1) {
            blocks1ToClear.forAllRowKeys(idx -> clearOneBlock_1(idx, recycler1));
        }

        /**
         * Clear one block.
         *
         * @param block2Idx the block index to clear
         * @param recycler recycler for lowest level blocks
         */
        private void clearOneLowestLevelBlock(final long block2Idx, final SoftRecycler<char[]> recycler) {
            final int block0 = (int) (block2Idx >> (BLOCK0_SHIFT - BLOCK2_SHIFT)) & BLOCK0_MASK;
            final int block1 = (int) (block2Idx >> (BLOCK1_SHIFT - BLOCK2_SHIFT)) & BLOCK1_MASK;
            final int block2 = (int) (block2Idx) & BLOCK2_MASK;

            final Block1 blocks0 = get(block0);
            if (blocks0 == null) {
                // we should not be asking to clear a block that does not actually exist
                throw new IllegalStateException();
            }

            final Block2 blocks1 = blocks0.get(block1);
            if (blocks1 == null) {
                // we should not be asking to clear a block that does not actually exist
                throw new IllegalStateException();
            }

            blocks1.clearByIndex(block2, recycler);
        }

        /**
         * Clear one block2 structure from a block1 structure.
         *
         * @param block1Idx the block index to clear
         * @param recycler recycler for lowest level blocks
         */
        private void clearOneBlock_2(final long block1Idx, final SoftRecycler<char[][]> recycler) {
            final int block0 = (int) (block1Idx >> (BLOCK0_SHIFT - BLOCK1_SHIFT)) & BLOCK0_MASK;
            final int block1 = (int) (block1Idx) & BLOCK1_MASK;

            final Block1 blocks0 = get(block0);
            if (blocks0 == null) {
                // we should not be asking to clear a block that does not actually exist
                throw new IllegalStateException();
            }

            blocks0.clearByIndex(block1, recycler);
        }

        /**
         * Clear one block1 structure from a block0 structure.
         *
         * <p>The silly underscore in the name is to make replication not append a &lt;T&gt; type in the object case.</p>
         *
         * @param block0Idx the block index to clear
         * @param recycler1 recycler for Block2[] arrays within the Block1 structure
         */
        private void clearOneBlock_1(final long block0Idx, final SoftRecycler<Block2[]> recycler1) {
            final int block0 = (int) (block0Idx) & BLOCK0_MASK;
            clearByIndex(block0, recycler1);
        }

        public char [] getInnermostBlockByKeyOrNull(long key) {
            final int block0 = (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;

            final Block1 blocks0 = get(block0);
            if (blocks0 == null) {
                return null;
            }

            final Block2 blocks1 = blocks0.get(block1);
            if (blocks1 == null) {
                return null;
            }
            return blocks1.get(block2);
        }

        public Block1 get(int idx) {
            if (oneIndex == idx) {
                return oneValue;
            }
            if (array == null) {
                return null;
            }
            return array[idx];
        }

        /**
         * Clears the given block index, which must exist
         *
         * @param idx       the index to clear
         * @param recycler1 the recycler for the array of Block2 inside the Block1 structure we are clearing
         */
        @SuppressWarnings("UnusedReturnValue")
        private void clearByIndex(final int idx, final SoftRecycler<Block2[]> recycler1) {
            Assert.neq(idx, "idx", HAVE_MORE_THAN_ONE, "HAVE_MORE_THAN_ONE");
            if (oneIndex == idx) {
                oneValue.maybeRecycle(recycler1);
                oneIndex = ONE_UNINITIALIZED;
                oneValue = null;
                return;
            }
            if (array == null || array[idx] == null) {
                throw new IllegalStateException();
            }
            array[idx].maybeRecycle(recycler1);
            array[idx] = null;
        }

        public void onEmptyResult(final SoftRecycler<Block1[]> recycler0) {
            maybeRecycle(recycler0);
            array = null;
            oneIndex = ONE_UNINITIALIZED;
        }

        public void set(int idx, Block1 value) {
            if (oneIndex == idx) {
                oneValue = value;
            } else {
                array[idx] = value;
            }
        }

        public void maybeRecycle(SoftRecycler<Block1 []> recycler) {
            if (array != null) {
                recycler.returnItem(array);
            }
        }

        public void enumerate(char nullValue, LongConsumer consumer) {
            if (oneIndex == ONE_UNINITIALIZED) {
                return;
            }
            if (oneIndex != HAVE_MORE_THAN_ONE) {
                if (oneValue != null) {
                    oneValue.enumerate(nullValue, consumer, (long) oneIndex << BLOCK0_SHIFT);
                }
                return;
            }
            for (int ii = 0; ii < BLOCK0_SIZE; ++ii) {
                if (array[ii] != null) {
                    array[ii].enumerate(nullValue, consumer, (long)ii << BLOCK0_SHIFT);
                }
            }
        }

        @TestUseOnly
        public long estimateSize() {
            if (oneIndex == ONE_UNINITIALIZED) {
                return 0;
            }
            if (oneIndex != HAVE_MORE_THAN_ONE) {
                return oneValue.estimateSize();
            }
            return (long)array.length * REFERENCE_SIZE + Arrays.stream(array).filter(Objects::nonNull).mapToLong(Block1::estimateSize).sum();
        }
    }
    // endregion Block0

    // region Block1
    static public final class Block1 {
        int oneIndex = ONE_UNINITIALIZED;
        Block2 oneValue;
        Block2 [] array;

        public void ensureIndex(int idx, SoftRecycler<Block2[]> recycler) {
            if (oneIndex == idx || oneIndex == HAVE_MORE_THAN_ONE) {
                return;
            }
            if (oneIndex == ONE_UNINITIALIZED) {
                oneIndex = idx;
            } else {
                if (recycler == null) {
                    array = new Block2[BLOCK1_SIZE];
                } else {
                    array = recycler.borrowItem();
                }
                array[oneIndex] = oneValue;
                oneIndex = HAVE_MORE_THAN_ONE;
                oneValue = null;
            }
        }

        public Block2 get(int idx) {
            if (oneIndex == idx) {
                return oneValue;
            }
            if (array == null) {
                return null;
            }
            return array[idx];
        }

        public void set(int idx, Block2 value) {
            if (oneIndex == idx) {
                oneValue = value;
            } else {
                array[idx] = value;
            }
        }

        public void maybeRecycle(SoftRecycler<Block2[]> recycler) {
            if (array != null) {
                recycler.returnItem(array);
            }
        }

        void enumerate(char nullValue, LongConsumer consumer, long offset) {
            if (oneIndex == ONE_UNINITIALIZED) {
                return;
            }
            if (oneIndex != HAVE_MORE_THAN_ONE) {
                if (oneValue != null) {
                    oneValue.enumerate(nullValue, consumer, offset + ((long)oneIndex << BLOCK1_SHIFT));
                }
                return;
            }
            for (int ii = 0; ii < BLOCK1_SIZE; ++ii) {
                if (array[ii] != null) {
                    array[ii].enumerate(nullValue, consumer, offset + ((long)ii << BLOCK1_SHIFT));
                }
            }
        }

        /**
         * Clears the given block index, which must exist
         *
         * @param idx       the index to clear
         * @param recycler2 the recycler for this array within a Block2 structure
         */
        private void clearByIndex(final int idx, final SoftRecycler<char[][]> recycler2) {
            Assert.neq(idx, "idx", HAVE_MORE_THAN_ONE, "HAVE_MORE_THAN_ONE");
            if (oneIndex == idx) {
                oneValue.maybeRecycle(recycler2);
                oneValue = null;
                oneIndex = ONE_UNINITIALIZED;
                return;
            }
            if (array == null || array[idx] == null) {
                throw new IllegalStateException();
            }
            array[idx].maybeRecycle(recycler2);
            array[idx] = null;
        }

        @TestUseOnly
        long estimateSize() {
            if (oneIndex == ONE_UNINITIALIZED) {
                return 0;
            }
            if (oneIndex != HAVE_MORE_THAN_ONE) {
                return oneValue.estimateSize();
            }
            return (long)array.length * REFERENCE_SIZE + Arrays.stream(array).filter(Objects::nonNull).mapToLong(Block2::estimateSize).sum();
        }
    }
    // endregion Block1

    // region Block2
    static public final class Block2 {
        int oneIndex = ONE_UNINITIALIZED;
        char [] oneValue;
        char [][] array;

        public void ensureIndex(int idx, SoftRecycler<char [][]> recycler) {
            if (oneIndex == idx || oneIndex == HAVE_MORE_THAN_ONE) {
                return;
            }
            if (oneIndex == ONE_UNINITIALIZED) {
                oneIndex = idx;
            } else {
                if (recycler == null) {
                    array = new char[BLOCK2_SIZE][];
                } else {
                    array = recycler.borrowItem();
                }
                array[oneIndex] = oneValue;
                oneIndex = HAVE_MORE_THAN_ONE;
                oneValue = null;
            }
        }

        public char [] get(int idx) {
            if (oneIndex == idx) {
                return oneValue;
            }
            if (array == null) {
                return null;
            }
            return array[idx];
        }

        public void set(int idx, char [] value) {
            if (oneIndex == idx) {
                oneValue = value;
            } else {
                array[idx] = value;
            }
        }

        public void maybeRecycle(SoftRecycler<char [][]> recycler) {
            if (array != null) {
                recycler.returnItem(array);
            }
        }

        void enumerate(char nullValue, LongConsumer consumer, long offset) {
            if (oneIndex == ONE_UNINITIALIZED) {
                return;
            }
            if (oneIndex != HAVE_MORE_THAN_ONE) {
                if (oneValue != null) {
                    enumerateInner(nullValue, oneValue, consumer, offset + ((long)oneIndex << BLOCK2_SHIFT));
                }
                return;
            }
            for (int ii = 0; ii < BLOCK2_SIZE; ++ii) {
                if (array[ii] != null) {
                    enumerateInner(nullValue, array[ii], consumer, offset + ((long)ii << BLOCK2_SHIFT));
                }
            }
        }

        private static void enumerateInner(char nullValue, char[] inner, LongConsumer consumer, long offset) {
            for (int ii = 0; ii < inner.length; ++ii) {
                if (inner[ii] != nullValue) {
                    consumer.accept(offset + ii);
                }
            }
        }

        /**
         * Clears the given block index, which must exist
         *
         * @param idx       the index to clear
         * @param recycler  the recycler for the block we are clearing
         */
        private void clearByIndex(final int idx,
                                     final SoftRecycler<char[]> recycler) {
            Assert.neq(idx, "idx", HAVE_MORE_THAN_ONE, "HAVE_MORE_THAN_ONE");
            if (oneIndex == idx) {
                recycler.returnItem(oneValue);
                oneValue = null;
                oneIndex = ONE_UNINITIALIZED;
                return;
            }
            if (array == null || array[idx] == null){
                // we should not be asking to clear a block that does not actually exist
                throw new IllegalStateException();
            }
            recycler.returnItem(array[idx]);
            array[idx] = null;
        }

        @TestUseOnly
        long estimateSize() {
            if (oneIndex == ONE_UNINITIALIZED) {
                return 0;
            }
            if (oneIndex != HAVE_MORE_THAN_ONE) {
                return (long)oneValue.length * Character.BYTES;
            }
            return (long)array.length * REFERENCE_SIZE + IntStream.range(0, BLOCK2_SIZE).filter(ii -> array[ii] != null).mapToLong(ii->(long)array[ii].length * ELEMENT_SIZE).sum();
        }
    }
    // endregion Block2
}
