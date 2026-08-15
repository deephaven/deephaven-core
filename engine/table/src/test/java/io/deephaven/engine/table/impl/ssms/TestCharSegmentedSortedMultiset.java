//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// @formatter:off
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ShiftObliviousListener;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.GenerateTableUpdates;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.value.iterator.ValueIteratorOfChar;
import io.deephaven.engine.table.impl.ssa.SsaTestHelpers;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.vector.CharVector;
import io.deephaven.vector.CharVectorDirect;
import io.deephaven.vector.ObjectVectorDirect;
import io.deephaven.engine.table.impl.util.compact.CharCompactKernel;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.TreeMap;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static org.junit.Assert.assertArrayEquals;

@Category(ParallelTest.class)
public class TestCharSegmentedSortedMultiset extends RefreshingTableTestCase {

    public void testInsertion() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 10; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, false, true);
                }
            }
        }
    }

    public void testRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        for (int seed = 0; seed < 10; ++seed) {
            for (int tableSize = 10; tableSize <= 1000; tableSize *= 10) {
                for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), false, true, true);
                }
            }
        }
    }

    public void testInsertAndRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final int nSeeds = scaleToDesiredTestLength(100);
        for (int tableSize = 10; tableSize <= 1000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < nSeeds; ++seed) {
                    testUpdates(desc.reset(seed, tableSize, nodeSize), true, true, true);
                }
            }
        }
    }

    public void testMove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final int nSeeds = scaleToDesiredTestLength(200);
        for (int tableSize = 10; tableSize <= 10000; tableSize *= 2) {
            for (int nodeSize = 8; nodeSize <= 2048; nodeSize *= 2) {
                for (int seed = 0; seed < nSeeds; ++seed) {
                    testMove(desc.reset(seed, tableSize, nodeSize), true);
                }
            }
        }
    }

    public void testEqualsArray() {
        // exercise the singleton (size == 1), single-leaf (partial, then exactly full), and multi-leaf (exactly two
        // full leaves, then several with a partial tail) representations
        checkEqualsArray(1);
        checkEqualsArray(3);
        checkEqualsArray(4);
        checkEqualsArray(8);
        checkEqualsArray(20);
    }

    public void testIterator() {
        // node sizes that put the same value counts in different representations
        for (final int nodeSize : new int[] {4, 8}) {
            // exercise the empty, singleton (size == 1), single-leaf, and multi-leaf representations
            for (final int valueCount : new int[] {0, 1, 3, 4, 8, 20}) {
                checkIterator(nodeSize, valueCount);
            }
        }
    }

    /**
     * hashCode() walks the leaves directly instead of delegating to the shared Vector helper, which duplicates that
     * helper's seed, multiplier, and per-element hash. Pin the two together across the empty, singleton, single-leaf,
     * and multi-leaf representations so the copy cannot drift -- equals() accepts any Vector with matching contents,
     * so a divergence here would silently break the hashCode contract.
     */
    public void testHashCodeMatchesVectorHelper() {
        for (final int valueCount : new int[] {0, 1, 3, 4, 8, 20}) {
            final char[] values = new char[valueCount];
            for (int ii = 0; ii < valueCount; ++ii) {
                values[ii] = (char) ('a' + ii);
            }
            final CharSegmentedSortedMultiset ssm = makeSsm(4, values);
            final String message = "valueCount=" + valueCount;

            // the helper applied to this SSM, to its materialized copy, and to an independently built Vector must all
            // agree with the walk
            assertEquals(message, CharVector.hashCode(ssm), ssm.hashCode());
            assertEquals(message, CharVector.hashCode(ssm.getDirect()), ssm.hashCode());
            assertEquals(message, new CharVectorDirect(values).hashCode(), ssm.hashCode());
        }
    }

    public void testMoveSingletonSource() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        // destination states: empty, singleton, partial leaf, full single leaf, multi-leaf (last leaf partial and full)
        for (final int destCount : new int[] {0, 1, 3, 4, 6, 8}) {
            checkAppendMaximum(nodeSize, destCount, desc);
            checkPrependMinimum(nodeSize, destCount, desc);
        }
    }

    public void testMoveSingletonMerge() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final char v = (char) ('a' + 5);
        final char w = (char) ('a' + 6);

        // moveFrontToBack: a singleton whose value equals the destination's (singleton) maximum merges via addMaxCount
        {
            final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {v}, new int[] {2});
            final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, new char[] {v}, new int[] {3});
            source.moveFrontToBack(dest, source.totalSize());
            verifySsm(source, new char[0], desc);
            verifySsm(dest, new char[] {v, v, v, v, v}, desc);
        }

        // moveBackToFront: a singleton whose value equals the destination's (singleton) minimum merges via addMinCount
        {
            final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {v}, new int[] {2});
            final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, new char[] {v}, new int[] {3});
            source.moveBackToFront(dest, source.totalSize());
            verifySsm(source, new char[0], desc);
            verifySsm(dest, new char[] {v, v, v, v, v}, desc);
        }

        // moveFrontToBack: a non-singleton source whose minimum equals the destination's maximum, moving fewer than the
        // minimum's count, reduces the source minimum in place (addMinCount(-count)) rather than removing it
        {
            final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {v, w}, new int[] {3, 1});
            final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, new char[] {v}, new int[] {1});
            source.moveFrontToBack(dest, 1);
            verifySsm(source, new char[] {v, v, w}, desc);
            verifySsm(dest, new char[] {v, v}, desc);
        }

        // moveBackToFront: the symmetric case, reducing the source maximum in place (addMaxCount(-count))
        {
            final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {v, w}, new int[] {1, 3});
            final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, new char[] {w}, new int[] {1});
            source.moveBackToFront(dest, 1);
            verifySsm(source, new char[] {v, w, w}, desc);
            verifySsm(dest, new char[] {w, w}, desc);
        }
    }

    public void testInsertIntoMiddleLeafSplit() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();

        // three full leaves at even offsets; inserting new odd-offset values into the first leaf overflows it, forcing a
        // hole that shifts the trailing leaves (copyLeavesAndDirectory) and runs the existing-value merge (maybeCompact)
        final char[] initial = new char[12];
        for (int ii = 0; ii < 12; ++ii) {
            initial[ii] = (char) ('a' + 2 * ii);
        }
        final CharSegmentedSortedMultiset ssm = makeSsm(nodeSize, initial);

        try (final WritableCharChunk<Values> values = WritableCharChunk.makeWritableChunk(3);
                final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(3)) {
            values.set(0, (char) ('a' + 1));
            values.set(1, (char) ('a' + 3));
            values.set(2, (char) ('a' + 5));
            counts.set(0, 1);
            counts.set(1, 1);
            counts.set(2, 1);
            ssm.insert(values, counts);
        }

        final char[] expected = new char[] {
                (char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3), (char) ('a' + 4),
                (char) ('a' + 5), (char) ('a' + 6), (char) ('a' + 8), (char) ('a' + 10), (char) ('a' + 12),
                (char) ('a' + 14), (char) ('a' + 16), (char) ('a' + 18), (char) ('a' + 20), (char) ('a' + 22)};
        verifySsm(ssm, expected, desc);
    }

    public void testRemoveMaxMultiLeaf() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();

        // a multi-leaf source whose maximum equals the destination minimum, moving the whole maximum entry, exercises
        // removeMax's leafCount > 1 branch
        final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {
                (char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3), (char) ('a' + 4)});
        final CharSegmentedSortedMultiset dest =
                makeSsm(nodeSize, new char[] {(char) ('a' + 4), (char) ('a' + 5)});
        source.moveBackToFront(dest, 1);
        verifySsm(source, new char[] {(char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3)}, desc);
        verifySsm(dest, new char[] {(char) ('a' + 4), (char) ('a' + 4), (char) ('a' + 5)}, desc);
    }

    public void testRemoveMaxSizeOne() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();

        // a two-value single-leaf directory; removing its maximum leaves a size-1 directory (removeMax's leafCount == 1
        // branch) rather than collapsing to the singleton representation
        final CharSegmentedSortedMultiset source =
                makeSsm(nodeSize, new char[] {(char) ('a' + 3), (char) ('a' + 4)});
        final CharSegmentedSortedMultiset dest1 =
                makeSsm(nodeSize, new char[] {(char) ('a' + 4), (char) ('a' + 5)});
        source.moveBackToFront(dest1, 1);
        verifySsm(source, new char[] {(char) ('a' + 3)}, desc);
        verifySsm(dest1, new char[] {(char) ('a' + 4), (char) ('a' + 4), (char) ('a' + 5)}, desc);

        // removing the maximum of that size-1 directory clears the set (removeMax's size == 1 branch)
        final CharSegmentedSortedMultiset dest2 =
                makeSsm(nodeSize, new char[] {(char) ('a' + 3), (char) ('a' + 6)});
        source.moveBackToFront(dest2, 1);
        verifySsm(source, new char[0], desc);
        verifySsm(dest2, new char[] {(char) ('a' + 3), (char) ('a' + 3), (char) ('a' + 6)}, desc);
    }

    public void testMoveFrontToBackPartialAppend() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();

        // single-leaf directory destination: moving part of the source minimum's count appends the partial value into
        // the destination directory (the directoryCount != null branch of the partial-append handling)
        {
            final CharSegmentedSortedMultiset source =
                    makeSsm(nodeSize, new char[] {(char) ('a' + 2), (char) ('a' + 3)}, new int[] {2, 2});
            final CharSegmentedSortedMultiset dest =
                    makeSsm(nodeSize, new char[] {(char) ('a' + 0), (char) ('a' + 1)});
            source.moveFrontToBack(dest, 1);
            verifySsm(source, new char[] {(char) ('a' + 2), (char) ('a' + 3), (char) ('a' + 3)}, desc);
            verifySsm(dest, new char[] {(char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2)}, desc);
        }

        // multi-leaf destination: the same partial move appends into the destination's last leaf, and the leftover count
        // is decremented on that leaf (the directoryCount == null leftover branch)
        {
            final CharSegmentedSortedMultiset source =
                    makeSsm(nodeSize, new char[] {(char) ('a' + 5), (char) ('a' + 6)}, new int[] {2, 2});
            final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, new char[] {
                    (char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3), (char) ('a' + 4)});
            source.moveFrontToBack(dest, 1);
            verifySsm(source, new char[] {(char) ('a' + 5), (char) ('a' + 6), (char) ('a' + 6)}, desc);
            verifySsm(dest, new char[] {(char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3),
                    (char) ('a' + 4), (char) ('a' + 5)}, desc);
        }
    }

    public void testMoveBackToFrontCompleteLeaves() {
        final int nodeSize = 4;
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();

        // a three-leaf source where the boundary value's count is split: moving the six largest transfers two complete
        // leaves plus a leftover slot of the boundary value (the multi-leaf complete-leaf move with a leftover slot)
        final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {
                (char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3), (char) ('a' + 4),
                (char) ('a' + 5), (char) ('a' + 6), (char) ('a' + 7), (char) ('a' + 8)},
                new int[] {1, 1, 1, 2, 1, 1, 1, 1, 1});
        final CharSegmentedSortedMultiset dest =
                makeSsm(nodeSize, new char[] {(char) ('a' + 9), (char) ('a' + 10)});
        source.moveBackToFront(dest, 6);
        verifySsm(source,
                new char[] {(char) ('a' + 0), (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3)}, desc);
        verifySsm(dest, new char[] {(char) ('a' + 3), (char) ('a' + 4), (char) ('a' + 5), (char) ('a' + 6),
                (char) ('a' + 7), (char) ('a' + 8), (char) ('a' + 9), (char) ('a' + 10)}, desc);
    }

    public void testScalarInsertRemove() {
        final SsaTestHelpers.TestDescriptor desc = new SsaTestHelpers.TestDescriptor();
        final int alphabet = 12;
        // a small node size so the scalar inserts/removes exercise the directory, multi-leaf, split, and collapse
        // paths with only a handful of distinct values
        final int nodeSize = 4;
        for (int seed = 0; seed < 20; ++seed) {
            final Random rng = new Random(seed);
            final CharSegmentedSortedMultiset ssm = new CharSegmentedSortedMultiset(nodeSize);
            final long[] refCounts = new long[alphabet];

            for (int ii = 0; ii < 60; ++ii) {
                final int offset = rng.nextInt(alphabet);
                final int count = 1 + rng.nextInt(3);
                final boolean wasPresent = refCounts[offset] > 0;
                final boolean added = ssm.insert((char) ('a' + offset), count);
                refCounts[offset] += count;
                assertEquals(!wasPresent, added);
                verifyScalar(ssm, refCounts, desc);
            }

            while (true) {
                final int[] present = new int[alphabet];
                int presentCount = 0;
                for (int offset = 0; offset < alphabet; ++offset) {
                    if (refCounts[offset] > 0) {
                        present[presentCount++] = offset;
                    }
                }
                if (presentCount == 0) {
                    break;
                }
                final int offset = present[rng.nextInt(presentCount)];
                final int count = 1 + rng.nextInt((int) refCounts[offset]);
                final boolean fully = count == refCounts[offset];
                final boolean removed = ssm.remove((char) ('a' + offset), count);
                refCounts[offset] -= count;
                assertEquals(fully, removed);
                verifyScalar(ssm, refCounts, desc);
            }

            assertEquals(0, ssm.size());
            assertEquals(0, ssm.totalSize());
        }
    }

    private void verifyScalar(CharSegmentedSortedMultiset ssm, long[] refCounts, SsaTestHelpers.TestDescriptor desc) {
        int total = 0;
        int distinct = 0;
        for (int offset = 0; offset < refCounts.length; ++offset) {
            total += refCounts[offset];
            if (refCounts[offset] > 0) {
                distinct++;
            }
        }
        final char[] expanded = new char[total];
        int position = 0;
        for (int offset = 0; offset < refCounts.length; ++offset) {
            for (long kk = 0; kk < refCounts[offset]; ++kk) {
                expanded[position++] = (char) ('a' + offset);
            }
        }
        verifySsm(ssm, expanded, desc);
        assertEquals(distinct, ssm.size());
        assertEquals(total, ssm.totalSize());
    }

    public void testInsertRemoveWithOffset() {
        final int nodeSize = 4;
        final int prefix = 3;
        // `subject` is built with offset inserts/removes (from chunks carrying a junk prefix that must be ignored);
        // `reference` is built with the plain (offset 0) calls. After every step the contents must be identical.
        final CharSegmentedSortedMultiset subject = new CharSegmentedSortedMultiset(nodeSize);
        final CharSegmentedSortedMultiset reference = new CharSegmentedSortedMultiset(nodeSize);

        // empty -> multiple leaves (makeLeavesInitial with an offset)
        applyInsert(subject, reference, prefix, range(1, 10), ones(10));
        // a new minimum plus merges into existing values (insertExisting + distributeNewIntoLeaves with an offset)
        applyInsert(subject, reference, prefix, new int[] {0, 1, 3}, new int[] {1, 2, 3});
        // new maximums (doAppend with an offset)
        applyInsert(subject, reference, prefix, new int[] {11, 12}, new int[] {5, 5});
        // removals from an offset (removeFromLeaf with an offset)
        applyRemove(subject, reference, prefix, new int[] {1, 3, 11}, new int[] {1, 2, 1});
    }

    /**
     * {@link CharSegmentedSortedMultiset#subVector} is exclusive at its end, and -- like every Vector -- must accept
     * offsets outside {@code [0, size())}, which read as the null value rather than throwing. Pin every representation
     * against {@link CharVectorDirect}, the reference implementation of that contract.
     */
    public void testPartialCopy() {
        // node sizes that put the same value counts in different representations
        for (final int nodeSize : new int[] {4, 8}) {
            // empty, singleton, partial leaf, exactly-full leaf, two full leaves, and many leaves
            for (final int valueCount : new int[] {0, 1, 3, 4, 8, 24}) {
                checkPartialCopy(nodeSize, valueCount);
            }
        }
    }

    private void checkPartialCopy(final int nodeSize, final int valueCount) {
        final char[] values = new char[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            values[ii] = (char) ('a' + ii);
        }
        final CharSegmentedSortedMultiset ssm = makeSsm(nodeSize, values);
        final CharVector reference = new CharVectorDirect(values);
        final String prefix = "nodeSize=" + nodeSize + ", valueCount=" + valueCount;

        assertArrayEquals(prefix, values, ssm.toArray()/*EXTRA*/);

        // an offset outside [0, size()) reads as null; it is neither an error nor a peek at a leaf's unused slots
        assertEquals(prefix, NULL_CHAR, ssm.get(-1));
        assertEquals(prefix, NULL_CHAR, ssm.get(valueCount));
        assertEquals(prefix, NULL_CHAR, ssm.get(valueCount + 1));
        assertEquals(prefix, NULL_CHAR, ssm.get(Long.MAX_VALUE));

        // sub-ranges that fall short of, span, and overrun each end
        for (int from = -3; from <= valueCount + 3; ++from) {
            for (int to = from; to <= valueCount + 3; ++to) {
                final String message = prefix + ", from=" + from + ", to=" + to;
                final CharVector expected = reference.subVector(from, to);
                final CharVector actual = ssm.subVector(from, to);
                assertEquals(message, to - from, actual.size());
                assertArrayEquals(message, expected.toArray(), actual.toArray()/*EXTRA*/);
                for (int ii = 0; ii < to - from; ++ii) {
                    assertEquals(message, expected.get(ii), actual.get(ii));
                }
            }
        }

        // positions are read individually, in the order given, may repeat, and may fall outside [0, size())
        final long[] positions =
                new long[] {valueCount - 1, -1, 0, valueCount, valueCount / 2, 0, Long.MAX_VALUE};
        assertArrayEquals(prefix, reference.subVectorByPositions(positions).toArray(),
                ssm.subVectorByPositions(positions).toArray()/*EXTRA*/);
    }

    // region SortFixupSanityCheck
    public void testSanity() {
        QueryTable john = TstUtils.testRefreshingTable(TableTools.charCol("John", NULL_CHAR, NULL_CHAR, (char)0x0, (char)0x1, Character.MAX_VALUE, Character.MAX_VALUE));
        final ColumnSource<Character> valueSource = john.getColumnSource("John");
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(1024);
             final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(1024);
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(1024)
        ) {
            valueSource.fillChunk(fillContext, chunk, john.getRowSet());
            CharCompactKernel.compactAndCount(chunk, counts, true, true);
        }
    }
    //endregion SortFixupSanityCheck

    private void testUpdates(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean allowAddition, boolean allowRemoval, boolean countNullNaN) {
        final Random random = new Random(desc.seed());
        final ColumnInfo[] columnInfo;
        final QueryTable table = getTable(desc.tableSize(), random, columnInfo = initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedMultiset ssm = new CharSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        checkSsmInitial(asCharacter, ssm, valueSource, countNullNaN, desc);

        try (final SafeCloseable ignored = LivenessScopeStack.open(new LivenessScope(true), true)) {
            final ShiftObliviousListener asCharacterListener = new ShiftObliviousInstrumentedListenerAdapter(asCharacter, false) {
                @Override
                public void onUpdate(RowSet added, RowSet removed, RowSet modified) {
                    final int maxSize = Math.max(Math.max(added.intSize(), removed.intSize()), modified.intSize());
                    try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(maxSize);
                         final WritableCharChunk<Values> chunk = WritableCharChunk.makeWritableChunk(maxSize);
                         final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(maxSize)
                    ) {
                        final SegmentedSortedMultiSet.RemoveContext removeContext = SegmentedSortedMultiSet.makeRemoveContext(desc.nodeSize());

                        if (removed.isNonempty()) {
                            valueSource.fillPrevChunk(fillContext, chunk, removed);
                            CharCompactKernel.compactAndCount(chunk, counts, countNullNaN, countNullNaN);
                            ssm.remove(removeContext, chunk, counts);
                        }


                        if (added.isNonempty()) {
                            valueSource.fillChunk(fillContext, chunk, added);
                            CharCompactKernel.compactAndCount(chunk, counts, countNullNaN, countNullNaN);
                            ssm.insert(chunk, counts);
                        }
                    }
                }
            };
            asCharacter.addUpdateListener(asCharacterListener);

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            while (desc.advance(50)) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    final RowSet[] notify = GenerateTableUpdates.computeTableUpdates(desc.tableSize(), random, table, columnInfo, allowAddition, allowRemoval, false);
                    assertTrue(notify[2].isEmpty());
                    table.notifyListeners(notify[0], notify[1], notify[2]);
                });

                try (final ColumnSource.GetContext getContext = valueSource.makeGetContext(asCharacter.intSize())) {
                    checkSsm(ssm, valueSource.getChunk(getContext, asCharacter.getRowSet()).asCharChunk(), countNullNaN, desc);
                }

                if (!allowAddition && table.size() == 0) {
                    break;
                }
            }
        }
    }

    private void testMove(@NotNull final SsaTestHelpers.TestDescriptor desc, boolean countNull) {
        final Random random = new Random(desc.seed());
        final QueryTable table = getTable(desc.tableSize(), random, initColumnInfos(new String[]{"Value"},
                SsaTestHelpers.getGeneratorForChar()));

        final Table asCharacter = SsaTestHelpers.prepareTestTableForChar(table);

        final CharSegmentedSortedMultiset ssmLo = new CharSegmentedSortedMultiset(desc.nodeSize());
        final CharSegmentedSortedMultiset ssmHi = new CharSegmentedSortedMultiset(desc.nodeSize());

        //noinspection unchecked
        final ColumnSource<Character> valueSource = asCharacter.getColumnSource("Value");

        checkSsmInitial(asCharacter, ssmLo, valueSource, countNull, desc);
        final long totalExpectedSize = ssmLo.totalSize();

        while (ssmLo.size() > 0) {
            desc.advance();
            try {
                final long count = random.nextInt(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()) + 1);
                final long newLoCount = ssmLo.totalSize() - count;
                final long newHiCount = ssmHi.totalSize() + count;
                if (printTableUpdates) {
                    System.out.println("Moving " + count + " of " + ssmLo.totalSize() + " elements.");
                }
                ssmLo.moveBackToFront(ssmHi, count);

                assertEquals(newLoCount, ssmLo.totalSize());
                assertEquals(newHiCount, ssmHi.totalSize());
                assertEquals(totalExpectedSize, ssmLo.totalSize() + ssmHi.totalSize());

            } catch (AssertionFailure e) {
                TestCase.fail("Moving lo to hi failed at " + desc + ": " + e.getMessage());
            }

            try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asCharacter.intSize());
                 final WritableCharChunk<Values> valueChunk = WritableCharChunk.makeWritableChunk(asCharacter.intSize())) {
                valueSource.fillChunk(fillContext, valueChunk, asCharacter.getRowSet());
                valueChunk.sort();
                final CharChunk<? extends Values> loValues = valueChunk.slice(0, LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()));
                final CharChunk<? extends Values> hiValues = valueChunk.slice(LongSizedDataStructure.intSize("ssmLo", ssmLo.totalSize()), LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()));
                checkSsm(ssmLo, loValues, countNull, desc);
                checkSsm(ssmHi, hiValues, countNull, desc);
            }
        }

        checkSsm(asCharacter, ssmHi, valueSource, countNull, desc);

        while (ssmHi.size() > 0) {
            desc.advance();
            try {
                final long count = random.nextInt(LongSizedDataStructure.intSize("ssmHi", ssmHi.totalSize()) + 1);

                final long newLoCount = ssmLo.totalSize() + count;
                final long newHiCount = ssmHi.totalSize() - count;

                if (printTableUpdates) {
                    System.out.println("Moving " + count + " of " + ssmHi.totalSize() + " elements.");
                }
                ssmHi.moveFrontToBack(ssmLo, count);

                assertEquals(newLoCount, ssmLo.totalSize());
                assertEquals(newHiCount, ssmHi.totalSize());
                assertEquals(totalExpectedSize, ssmLo.totalSize() + ssmHi.totalSize());
            } catch (AssertionFailure e) {
                TestCase.fail("Moving hi to lo failed at " + desc + ": " + e.getMessage());
            }
        }

        checkSsm(asCharacter, ssmLo, valueSource, countNull, desc);
    }

    private void checkSsmInitial(Table asCharacter, CharSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNullNaN, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asCharacter.intSize());
             final WritableCharChunk<Values> valueChunk = WritableCharChunk.makeWritableChunk(asCharacter.intSize());
             final WritableIntChunk<ChunkLengths> counts = WritableIntChunk.makeWritableChunk(asCharacter.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asCharacter.getRowSet());
            valueChunk.sort();

            CharCompactKernel.compactAndCount(valueChunk, counts, countNullNaN, countNullNaN);

            ssm.insert(valueChunk, counts);

            valueSource.fillChunk(fillContext, valueChunk, asCharacter.getRowSet());
            checkSsm(ssm, valueChunk, countNullNaN, desc);
        }
    }

    private void checkSsm(Table asCharacter, CharSegmentedSortedMultiset ssm, ColumnSource<?> valueSource, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try (final ColumnSource.FillContext fillContext = valueSource.makeFillContext(asCharacter.intSize());
             final WritableCharChunk<Values> valueChunk = WritableCharChunk.makeWritableChunk(asCharacter.intSize())) {
            valueSource.fillChunk(fillContext, valueChunk, asCharacter.getRowSet());
            checkSsm(ssm, valueChunk, countNull, desc);
        }
    }

    private void checkSsm(CharSegmentedSortedMultiset ssm, CharChunk<? extends Values> valueChunk, boolean countNull, @NotNull final SsaTestHelpers.TestDescriptor desc) {
        try {
            ssm.validate();
            try (final WritableCharChunk<?> keys = ssm.keyChunk();
                 final WritableLongChunk<?> counts = ssm.countChunk()) {

                int totalSize = 0;

                final Map<Character, Integer> checkMap = new TreeMap<>(CharComparisons::compare);
                for (int ii = 0; ii < valueChunk.size(); ++ii) {
                    final char value = valueChunk.get(ii);
                    if (value == NULL_CHAR && !countNull) {
                        continue;
                    }
                    totalSize++;
                    checkMap.compute(value, (key, cnt) -> {
                        if (cnt == null) return 1;
                        else return cnt + 1;
                    });
                }

                assertEquals(checkMap.size(), ssm.size());
                assertEquals(totalSize, ssm.totalSize());
                assertEquals(checkMap.size(), keys.size());
                assertEquals(checkMap.size(), counts.size());

                final MutableInt offset = new MutableInt(0);
                checkMap.forEach((key, count) -> {
                    assertEquals((char) key, keys.get(offset.get()));
                    assertEquals((long) count, counts.get(offset.get()));
                    offset.increment();
                });
            }
        } catch (AssertionFailure e) {
            TestCase.fail("Check failed at " + desc + ": " + e.getMessage());
        }
    }

    private CharSegmentedSortedMultiset makeSsm(int nodeSize, char[] values) {
        final int[] counts = new int[values.length];
        Arrays.fill(counts, 1);
        return makeSsm(nodeSize, values, counts);
    }

    private CharSegmentedSortedMultiset makeSsm(int nodeSize, char[] values, int[] counts) {
        final CharSegmentedSortedMultiset ssm = new CharSegmentedSortedMultiset(nodeSize);
        if (values.length > 0) {
            try (final WritableCharChunk<Values> valuesChunk = WritableCharChunk.makeWritableChunk(values.length);
                 final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(values.length)) {
                for (int ii = 0; ii < values.length; ++ii) {
                    valuesChunk.set(ii, values[ii]);
                    countsChunk.set(ii, counts[ii]);
                }
                ssm.insert(valuesChunk, countsChunk);
            }
        }
        return ssm;
    }

    private static int[] range(int start, int count) {
        final int[] result = new int[count];
        for (int ii = 0; ii < count; ++ii) {
            result[ii] = start + ii;
        }
        return result;
    }

    private static int[] ones(int count) {
        final int[] result = new int[count];
        Arrays.fill(result, 1);
        return result;
    }

    private void applyInsert(CharSegmentedSortedMultiset subject, CharSegmentedSortedMultiset reference, int prefix,
            int[] valueOffsets, int[] counts) {
        final char[] values = new char[valueOffsets.length];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = (char) ('a' + valueOffsets[ii]);
        }

        // reference: a plain (offset 0) insert
        try (final WritableCharChunk<Values> valuesChunk = WritableCharChunk.makeWritableChunk(values.length);
             final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(values.length)) {
            for (int ii = 0; ii < values.length; ++ii) {
                valuesChunk.set(ii, values[ii]);
                countsChunk.set(ii, counts[ii]);
            }
            reference.insert(valuesChunk, countsChunk);
        }

        // subject: an offset insert from a chunk with a junk prefix that must be left untouched
        final char junk = (char) ('a' - 1);
        try (final WritableCharChunk<Values> valuesChunk = WritableCharChunk.makeWritableChunk(prefix + values.length);
             final WritableIntChunk<ChunkLengths> countsChunk =
                     WritableIntChunk.makeWritableChunk(prefix + values.length)) {
            for (int ii = 0; ii < prefix; ++ii) {
                valuesChunk.set(ii, junk);
                countsChunk.set(ii, 7);
            }
            for (int ii = 0; ii < values.length; ++ii) {
                valuesChunk.set(prefix + ii, values[ii]);
                countsChunk.set(prefix + ii, counts[ii]);
            }
            subject.insert(valuesChunk, countsChunk, prefix, values.length);
            for (int ii = 0; ii < prefix; ++ii) {
                assertEquals(junk, valuesChunk.get(ii));
            }
        }

        assertSameContents(subject, reference);
    }

    private void applyRemove(CharSegmentedSortedMultiset subject, CharSegmentedSortedMultiset reference, int prefix,
            int[] valueOffsets, int[] counts) {
        final char[] values = new char[valueOffsets.length];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = (char) ('a' + valueOffsets[ii]);
        }
        final SegmentedSortedMultiSet.RemoveContext removeContext =
                SegmentedSortedMultiSet.makeRemoveContext(reference.getNodeSize());

        try (final WritableCharChunk<Values> valuesChunk = WritableCharChunk.makeWritableChunk(values.length);
             final WritableIntChunk<ChunkLengths> countsChunk = WritableIntChunk.makeWritableChunk(values.length)) {
            for (int ii = 0; ii < values.length; ++ii) {
                valuesChunk.set(ii, values[ii]);
                countsChunk.set(ii, counts[ii]);
            }
            reference.remove(removeContext, valuesChunk, countsChunk);
        }

        final char junk = (char) ('a' - 1);
        try (final WritableCharChunk<Values> valuesChunk = WritableCharChunk.makeWritableChunk(prefix + values.length);
             final WritableIntChunk<ChunkLengths> countsChunk =
                     WritableIntChunk.makeWritableChunk(prefix + values.length)) {
            for (int ii = 0; ii < prefix; ++ii) {
                valuesChunk.set(ii, junk);
                countsChunk.set(ii, 7);
            }
            for (int ii = 0; ii < values.length; ++ii) {
                valuesChunk.set(prefix + ii, values[ii]);
                countsChunk.set(prefix + ii, counts[ii]);
            }
            subject.remove(removeContext, valuesChunk, countsChunk, prefix, values.length);
            for (int ii = 0; ii < prefix; ++ii) {
                assertEquals(junk, valuesChunk.get(ii));
            }
        }

        assertSameContents(subject, reference);
    }

    private void assertSameContents(CharSegmentedSortedMultiset subject, CharSegmentedSortedMultiset reference) {
        subject.validate();
        reference.validate();
        assertEquals(reference.size(), subject.size());
        assertEquals(reference.totalSize(), subject.totalSize());
        try (final WritableCharChunk<?> subjectKeys = subject.keyChunk();
             final WritableLongChunk<?> subjectCounts = subject.countChunk();
             final WritableCharChunk<?> referenceKeys = reference.keyChunk();
             final WritableLongChunk<?> referenceCounts = reference.countChunk()) {
            assertEquals(referenceKeys.size(), subjectKeys.size());
            for (int ii = 0; ii < referenceKeys.size(); ++ii) {
                assertEquals(referenceKeys.get(ii), subjectKeys.get(ii));
                assertEquals(referenceCounts.get(ii), subjectCounts.get(ii));
            }
        }
    }

    private void checkEqualsArray(int valueCount) {
        final int nodeSize = 4;
        final char[] values = new char[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            values[ii] = (char) ('a' + ii);
        }
        final CharSegmentedSortedMultiset ssm = makeSsm(nodeSize, values);

        final Character[] boxed = new Character[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            boxed[ii] = values[ii];
        }

        // a Vector with identical contents is equal, in both directions, and anything equal must hash alike -- so the
        // SSM has to use the same shared Vector helper that the *VectorDirect implementations use, and compare
        // elements the same way that helper hashes them, rather than either with a scheme of its own
        assertEqualBothWays(ssm, ssm.getDirect());
        assertEqualBothWays(ssm, new CharVectorDirect(values));

        // the boxed comparison only runs in one direction for the primitive variants, because a primitive SSM is a
        // CharVector rather than an ObjectVector and so is not a comparand ObjectVectorDirect will accept
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxed)));
        assertEquals(ssm.hashCode(), new ObjectVectorDirect<>(boxed).hashCode());

        // another SSM holding the same values is equal however those values happen to be laid out: equality is a
        // property of the contents, and identical contents can occupy different leaf structures, since leaves need
        // not be full and the two node sizes need not agree
        assertEqualBothWays(ssm, makeSsm(nodeSize, values));
        assertEqualBothWays(ssm, makeSsm(nodeSize * 16, values));

        // an SSM of the same length holding different values is not equal, under either layout
        final char[] shifted = new char[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            shifted[ii] = (char) ('a' + ii + 1);
        }
        assertNotEqualBothWays(ssm, makeSsm(nodeSize, shifted));
        assertNotEqualBothWays(ssm, makeSsm(nodeSize * 16, shifted));

        // ... and neither is a longer one
        final char[] longerValues = new char[valueCount + 1];
        for (int ii = 0; ii < longerValues.length; ++ii) {
            longerValues[ii] = (char) ('a' + ii);
        }
        assertNotEqualBothWays(ssm, makeSsm(nodeSize, longerValues));
        assertNotEqualBothWays(ssm, makeSsm(nodeSize * 16, longerValues));

        // a Vector of a different length is not equal
        final char[] longer = new char[valueCount + 1];
        for (int ii = 0; ii < longer.length; ++ii) {
            longer[ii] = (char) ('a' + ii);
        }
        assertFalse(ssm.equals(new CharVectorDirect(longer)));
        final Character[] longerBoxed = new Character[valueCount + 1];
        for (int ii = 0; ii < longerBoxed.length; ++ii) {
            longerBoxed[ii] = (char) ('a' + ii);
        }
        assertFalse(ssm.equals(new ObjectVectorDirect<>(longerBoxed)));

        // a Vector that differs from the original in a single position is not equal; check the first, middle, and last
        if (valueCount > 0) {
            final char different = (char) ('a' + 20);
            for (final int position : new int[] {0, valueCount / 2, valueCount - 1}) {
                final char[] modifiedValues = values.clone();
                modifiedValues[position] = different;
                assertFalse(ssm.equals(new CharVectorDirect(modifiedValues)));

                final Character[] modifiedBoxed = boxed.clone();
                modifiedBoxed[position] = different;
                assertFalse(ssm.equals(new ObjectVectorDirect<>(modifiedBoxed)));
            }
        }
    }

    /**
     * Assert that two Vectors agree that they are equal no matter which is the receiver, and that they hash alike as
     * {@link Object#hashCode()} then requires.
     */
    private void assertEqualBothWays(Object lhs, Object rhs) {
        assertTrue(lhs + " should equal " + rhs, lhs.equals(rhs));
        assertTrue(rhs + " should equal " + lhs, rhs.equals(lhs));
        assertEquals("equal values must hash alike", lhs.hashCode(), rhs.hashCode());
    }

    /**
     * Assert that two Vectors agree that they are unequal no matter which is the receiver. Their hash codes are
     * unconstrained -- unequal values are permitted to collide.
     */
    private void assertNotEqualBothWays(Object lhs, Object rhs) {
        assertFalse(lhs + " should not equal " + rhs, lhs.equals(rhs));
        assertFalse(rhs + " should not equal " + lhs, rhs.equals(lhs));
    }

    private void checkIterator(final int nodeSize, final int valueCount) {
        final char[] values = new char[valueCount];
        for (int ii = 0; ii < valueCount; ++ii) {
            values[ii] = (char) ('a' + ii);
        }
        final CharSegmentedSortedMultiset ssm = makeSsm(nodeSize, values);
        // the reference implementation of the slice contract, including the null values owed for offsets outside
        // [0, size())
        final CharVector reference = new CharVectorDirect(values);
        final String prefix = "nodeSize=" + nodeSize + ", valueCount=" + valueCount;

        // a full traversal must visit every element in order
        try (final ValueIteratorOfChar it = ssm.iterator()) {
            assertEquals(prefix, valueCount, it.remaining());
            for (int ii = 0; ii < valueCount; ++ii) {
                assertTrue(prefix, it.hasNext());
                assertEquals(prefix, values[ii], it.nextChar());
            }
            assertFalse(prefix, it.hasNext());
        }

        // every sub-range must resolve its starting leaf correctly and stop at the right position; for a multi-leaf
        // SSM the start may land mid-leaf, on a leaf boundary, or past several whole leaves. Ranges that fall outside
        // [0, size()) are legal, and iterate as null at those offsets.
        for (int from = -3; from <= valueCount + 3; ++from) {
            for (int to = from; to <= valueCount + 3; ++to) {
                final String message = prefix + ", from=" + from + ", to=" + to;
                try (final ValueIteratorOfChar it = ssm.iterator(from, to)) {
                    assertEquals(message, to - from, it.remaining());
                    for (int ii = from; ii < to; ++ii) {
                        assertTrue(message, it.hasNext());
                        assertEquals(message, reference.get(ii), it.nextChar());
                        assertEquals(message, to - ii - 1, it.remaining());
                    }
                    assertFalse(message, it.hasNext());

                    // an exhausted iterator must not hand back whatever value happens to be stored next
                    try {
                        it.nextChar();
                        fail(message + ": expected a NoSuchElementException from an exhausted iterator");
                    } catch (NoSuchElementException expected) {
                        // expected
                    }
                }

                // documented equivalence: iterator(from, to) matches subVector(from, to).iterator()
                try (final ValueIteratorOfChar it = ssm.iterator(from, to);
                     final ValueIteratorOfChar sliceIt = ssm.subVector(from, to).iterator()) {
                    while (sliceIt.hasNext()) {
                        assertTrue(message, it.hasNext());
                        assertEquals(message, sliceIt.nextChar(), it.nextChar());
                    }
                    assertFalse(message, it.hasNext());
                }
            }
        }
    }

    // region NullEquals
    public void testEqualsArrayNull() {
        // a singleton holding the null sentinel
        checkEqualsArrayNull(4, new char[] {NULL_CHAR});
        // a single leaf containing the null sentinel
        checkEqualsArrayNull(4, new char[] {NULL_CHAR, (char) ('a' + 1), (char) ('a' + 2)});
        // multiple leaves containing the null sentinel
        checkEqualsArrayNull(4, new char[] {NULL_CHAR,
                (char) ('a' + 1), (char) ('a' + 2), (char) ('a' + 3), (char) ('a' + 4), (char) ('a' + 5)});
    }

    private void checkEqualsArrayNull(int nodeSize, char[] sortedValues) {
        // sortedValues must be sorted and distinct and contain the null sentinel (which sorts first)
        final CharSegmentedSortedMultiset ssm = makeSsm(nodeSize, sortedValues);
        final char[] stored = ssm.toArray();

        // boxing the null sentinel yields a non-null element holding the sentinel value, which compares equal
        final Character[] boxedSentinel = new Character[stored.length];
        for (int ii = 0; ii < stored.length; ++ii) {
            boxedSentinel[ii] = stored[ii];
        }
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxedSentinel)));

        // a literal null also compares equal to the stored null sentinel
        final Character[] boxedNull = boxedSentinel.clone();
        for (int ii = 0; ii < stored.length; ++ii) {
            if (stored[ii] == NULL_CHAR) {
                boxedNull[ii] = null;
            }
        }
        assertTrue(ssm.equals(new ObjectVectorDirect<>(boxedNull)));

        // a null at a position holding a non-null value is not equal
        for (int ii = 0; ii < stored.length; ++ii) {
            if (stored[ii] != NULL_CHAR) {
                final Character[] boxedWrongNull = boxedSentinel.clone();
                boxedWrongNull[ii] = null;
                assertFalse(ssm.equals(new ObjectVectorDirect<>(boxedWrongNull)));
                break;
            }
        }
    }
    // endregion NullEquals

    private void verifySsm(CharSegmentedSortedMultiset ssm, char[] expanded, SsaTestHelpers.TestDescriptor desc) {
        try (final WritableCharChunk<Values> valueChunk =
                WritableCharChunk.makeWritableChunk(Math.max(expanded.length, 1))) {
            valueChunk.setSize(expanded.length);
            for (int ii = 0; ii < expanded.length; ++ii) {
                valueChunk.set(ii, expanded[ii]);
            }
            checkSsm(ssm, valueChunk, true, desc);
        }
    }

    private void checkAppendMaximum(int nodeSize, int destCount, SsaTestHelpers.TestDescriptor desc) {
        final char[] destValues = new char[destCount];
        for (int ii = 0; ii < destCount; ++ii) {
            destValues[ii] = (char) ('a' + 1 + ii);
        }
        // strictly greater than everything in the destination, so it becomes the new maximum (exercises appendMaximum)
        final char value = (char) ('a' + 20);

        final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {value});
        final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, destValues);

        source.moveFrontToBack(dest, source.totalSize());

        verifySsm(source, new char[0], desc);
        final char[] expected = Arrays.copyOf(destValues, destCount + 1);
        expected[destCount] = value;
        verifySsm(dest, expected, desc);
    }

    private void checkPrependMinimum(int nodeSize, int destCount, SsaTestHelpers.TestDescriptor desc) {
        final char[] destValues = new char[destCount];
        for (int ii = 0; ii < destCount; ++ii) {
            destValues[ii] = (char) ('a' + 1 + ii);
        }
        // strictly less than everything in the destination, so it becomes the new minimum (exercises prependMinimum).
        // Use ('a' + 0) rather than a bare 'a' so that the Object replication boxes to the same type as the other
        // values (int arithmetic boxes to Integer; a bare char literal would box to Character and break comparisons).
        final char value = (char) ('a' + 0);

        final CharSegmentedSortedMultiset source = makeSsm(nodeSize, new char[] {value});
        final CharSegmentedSortedMultiset dest = makeSsm(nodeSize, destValues);

        source.moveBackToFront(dest, source.totalSize());

        verifySsm(source, new char[0], desc);
        final char[] expected = new char[destCount + 1];
        expected[0] = value;
        System.arraycopy(destValues, 0, expected, 1, destCount);
        verifySsm(dest, expected, desc);
    }
}
