/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util;

import io.deephaven.base.Pair;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.ChunkUtils;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.type.ArrayTypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class TableDiff {
    private static final int chunkSize = 1 << 16;
    private static final EnumSet<DiffItems> DOUBLES_EXACT_AND_FRACTION =
            EnumSet.of(DiffItems.DoublesExact, DiffItems.DoubleFraction);
    private static final double DOUBLE_EXACT_THRESHOLD = 0.0001;
    private static final double FLOAT_EXACT_THRESHOLD = 0.005;

    /**
     * Show the differences between two tables.
     *
     * @param actualResult the actual result from an operation
     * @param expectedResult the expected result from the operation
     * @param maxDiffLines the maximum number of lines in the output
     * @param itemsToSkip classes of changes to ignore
     * @return a pair containing an error description String and the first different line
     */
    @NotNull
    static Pair<String, Long> diffInternal(Table actualResult, Table expectedResult, long maxDiffLines,
            EnumSet<DiffItems> itemsToSkip) {
        final List<String> issues = new ArrayList<>();
        long firstDifferentPosition = Long.MAX_VALUE;

        if (expectedResult == null) {
            throw new IllegalArgumentException("Can not pass null expected result to TableTools.diff!");
        }

        if (actualResult == null) {
            issues.add("Actual result is null!");
            return makeResult(issues, maxDiffLines, firstDifferentPosition);
        }

        if (actualResult.size() != expectedResult.size()) {
            issues.add("Result table has size " + actualResult.size() + " vs. expected " + expectedResult.size());
            if (issues.size() >= maxDiffLines) {
                return makeResult(issues, maxDiffLines, firstDifferentPosition);
            }
        }

        final Map<String, ? extends ColumnSource> actualNameToColumnSource = actualResult.getColumnSourceMap();
        final Map<String, ? extends ColumnSource> expectedNameToColumnSource = expectedResult.getColumnSourceMap();
        final String[] actualColumnNames =
                actualResult.getDefinition().getColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        final String[] expectedColumnNames =
                expectedResult.getDefinition().getColumnNames().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);

        for (final String actualColumnName : actualColumnNames) {
            if (!expectedNameToColumnSource.containsKey(actualColumnName)) {
                issues.add("Column " + actualColumnName + " not expected");
                if (issues.size() >= maxDiffLines) {
                    return makeResult(issues, maxDiffLines, firstDifferentPosition);
                }
            }
        }

        final Set<String> columnNamesForDiff = new LinkedHashSet<>();
        for (int ci = 0; ci < expectedColumnNames.length; ci++) {
            final String expectedColumnName = expectedColumnNames[ci];
            final ColumnSource expectedColumnSource = expectedNameToColumnSource.get(expectedColumnName);
            final ColumnSource actualColumnSource = actualNameToColumnSource.get(expectedColumnName);
            if (actualColumnSource == null) {
                issues.add("Expected column " + expectedColumnName + " not found");
            } else {
                if (actualColumnNames.length - 1 < ci) {
                    if (!itemsToSkip.contains(DiffItems.ColumnsOrder)) {
                        issues.add("Expected column " + expectedColumnName + " is found but not on expected position ("
                                + ci + ")");
                    }
                } else {
                    if (!expectedColumnName.equals(actualColumnNames[ci])) {
                        if (!itemsToSkip.contains(DiffItems.ColumnsOrder)) {
                            issues.add("Expected column " + expectedColumnName
                                    + " is found but not on expected position (" + ci + ")");
                        }
                    }
                }

                final Class<?> expectedType = expectedColumnSource.getType();
                final Class<?> actualType = actualColumnSource.getType();
                if (actualType != expectedType) {
                    issues.add("Expected type of " + expectedColumnName + " is " + expectedType + " actual type is "
                            + actualType);
                } else {
                    columnNamesForDiff.add(expectedColumnName);
                }
            }
            if (issues.size() >= maxDiffLines) {
                return makeResult(issues, maxDiffLines, firstDifferentPosition);
            }
        }

        try (final SafeCloseableList safeCloseables = new SafeCloseableList();
                final SharedContext expectedSharedContext = SharedContext.makeSharedContext();
                final SharedContext actualSharedContext = SharedContext.makeSharedContext();
                final WritableBooleanChunk equalValues = WritableBooleanChunk.makeWritableChunk(chunkSize)) {

            final ColumnDiffContext[] columnContexts = columnNamesForDiff.stream()
                    .map(name -> safeCloseables.add(new ColumnDiffContext(name, expectedNameToColumnSource.get(name),
                            expectedSharedContext, actualNameToColumnSource.get(name), actualSharedContext)))
                    .toArray(ColumnDiffContext[]::new);

            try (final RowSequence.Iterator expectedIterator = expectedResult.getRowSet().getRowSequenceIterator();
                    final RowSequence.Iterator actualIterator = actualResult.getRowSet().getRowSequenceIterator()) {

                int columnsRemaining = columnContexts.length;
                long position = 0;
                while (expectedIterator.hasMore() && actualIterator.hasMore() && columnsRemaining > 0) {
                    final RowSequence expectedChunkOk = expectedIterator.getNextRowSequenceWithLength(chunkSize);
                    final RowSequence actualChunkOk = actualIterator.getNextRowSequenceWithLength(chunkSize);

                    for (int ci = 0; ci < columnContexts.length; ++ci) {
                        final ColumnDiffContext columnContext = columnContexts[ci];
                        if (columnContext == null) {
                            continue;
                        }
                        final long columnFirstDifferentPosition = columnContext.diffChunk(expectedChunkOk,
                                actualChunkOk, equalValues, itemsToSkip, issues, position);
                        if (columnFirstDifferentPosition == -1L) {
                            continue;
                        }
                        --columnsRemaining;
                        columnContexts[ci] = null;
                        firstDifferentPosition = Math.min(columnFirstDifferentPosition, firstDifferentPosition);
                        if (issues.size() >= maxDiffLines) {
                            return makeResult(issues, maxDiffLines, firstDifferentPosition);
                        }
                    }

                    expectedSharedContext.reset();
                    actualSharedContext.reset();
                    position += chunkSize;
                }
            }
        }
        return makeResult(issues, maxDiffLines, firstDifferentPosition);
    }

    private static Pair<String, Long> makeResult(@NotNull final List<String> issues, final long maxDiffLines,
            final long firstDifferentPosition) {
        final StringBuilder result = new StringBuilder();
        int count = 0;
        for (final String issue : issues) {
            if (count > maxDiffLines) {
                result.append("... and ").append(issues.size() - count).append(" more issues");
                break;
            }
            result.append(issue).append("\n");
            count++;
        }
        // noinspection AutoBoxing
        return new Pair<>(result.toString(), firstDifferentPosition == Long.MAX_VALUE ? 0 : firstDifferentPosition);
    }

    /**
     * A set of differences that are skipped.
     */
    public enum DiffItems {
        /**
         * Doubles and Floats are not treated as differences if they are within {@link #DOUBLE_EXACT_THRESHOLD} or
         * {@link #FLOAT_EXACT_THRESHOLD}.
         */
        DoublesExact,
        /**
         * Columns that exist in both tables, but in diferent orders are not treated as differences.
         */
        ColumnsOrder,
        /**
         * Doubles and Floats are not treated as differences if they are within a factor of
         * {@link #DOUBLE_EXACT_THRESHOLD} or {@link #FLOAT_EXACT_THRESHOLD}. DoublesExact must also be set.
         */
        DoubleFraction
    }

    private static final class ColumnDiffContext implements Context {

        private final String name;
        private final ColumnSource<?> expectedColumnSource;
        private final ColumnSource.GetContext expectedContext;
        private final ColumnSource<?> actualColumnSource;
        private final ColumnSource.GetContext actualContext;
        private final ChunkType chunkType;
        private final ChunkEquals chunkEquals;

        private ColumnDiffContext(@NotNull final String name,
                @NotNull final ColumnSource<?> expectedColumnSource,
                @NotNull final SharedContext expectedSharedContext,
                @NotNull final ColumnSource<?> actualColumnSource,
                @NotNull final SharedContext actualSharedContext) {
            this.name = name;
            this.expectedColumnSource = expectedColumnSource;
            expectedContext = expectedColumnSource.makeGetContext(chunkSize, expectedSharedContext);
            this.actualColumnSource = actualColumnSource;
            actualContext = actualColumnSource.makeGetContext(chunkSize, actualSharedContext);
            chunkType = expectedColumnSource.getChunkType();
            chunkEquals = ChunkEquals.makeEqual(chunkType);
        }

        /**
         * Diff a chunk's worth of values between the expected and actual column sources.
         *
         * @param expectedChunkOk The keys to get from the expected source
         * @param actualChunkOk The keys to get from the actual source
         * @param equalValues A place to store equality results
         * @param itemsToSkip {@link DiffItems} to skip
         * @param issues A place to record issues
         * @param position The row number to start from, 0-indexed
         * @return -1 if the expected and actual chunks were equal, else the position in row space of the first
         *         difference
         */
        private long diffChunk(@NotNull final RowSequence expectedChunkOk,
                @NotNull final RowSequence actualChunkOk,
                @NotNull final WritableBooleanChunk equalValues,
                @NotNull final Set<DiffItems> itemsToSkip,
                @NotNull final List<String> issues,
                long position) {
            final Chunk<? extends Values> expectedValues =
                    expectedColumnSource.getChunk(expectedContext, expectedChunkOk);
            final Chunk<? extends Values> actualValues = actualColumnSource.getChunk(actualContext, actualChunkOk);

            if (expectedValues.size() < actualValues.size()) {
                chunkEquals.equal(expectedValues, actualValues, equalValues);
            } else {
                chunkEquals.equal(actualValues, expectedValues, equalValues);
            }

            for (int ii = 0; ii < equalValues.size(); ++ii, ++position) {
                if (equalValues.get(ii)) {
                    continue;
                }
                if (chunkType == ChunkType.Object) {
                    final Object expectedValue = expectedValues.asObjectChunk().get(ii);
                    final Object actualValue = actualValues.asObjectChunk().get(ii);

                    if (actualValue == null || expectedValue == null || !actualValue.getClass().isArray()) {
                        issues.add("Column " + name + " different from the expected set, first difference at row " +
                                position + " encountered " + actualValue + " expected " + expectedValue);
                        return position;
                    }
                    if (!ArrayTypeUtils.equals(actualValue, expectedValue)) {
                        issues.add("Column " + name + " different from the expected set, first difference at row " +
                                position + " encountered " + ArrayTypeUtils.toString(actualValue) + " expected "
                                + ArrayTypeUtils.toString(expectedValue));
                        return position;
                    }
                } else if (chunkType == ChunkType.Float) {
                    final float expectedValue = expectedValues.asFloatChunk().get(ii);
                    final float actualValue = actualValues.asFloatChunk().get(ii);
                    if (expectedValue == io.deephaven.util.QueryConstants.NULL_FLOAT
                            || actualValue == io.deephaven.util.QueryConstants.NULL_FLOAT) {
                        final String actualString = actualValue == io.deephaven.util.QueryConstants.NULL_FLOAT ? "null"
                                : Float.toString(actualValue);
                        final String expectString =
                                expectedValue == io.deephaven.util.QueryConstants.NULL_FLOAT ? "null"
                                        : Float.toString(expectedValue);
                        issues.add("Column " + name + " different from the expected set, first difference at row " +
                                position + " encountered " + actualString + " expected " + expectString);
                        return position;
                    }
                    final float difference = Math.abs(expectedValue - actualValue);
                    if (itemsToSkip.containsAll(DOUBLES_EXACT_AND_FRACTION)) {
                        final float fracDiff = difference / Math.min(Math.abs(expectedValue), Math.abs(actualValue));
                        // if we are different by more than 0.5%, then we have an error; otherwise it is within bounds
                        if (fracDiff > FLOAT_EXACT_THRESHOLD) {
                            issues.add("Column " + name + " different from the expected set, first difference at row " +
                                    position + " encountered " + actualValue + " expected " + expectedValue
                                    + " (difference = " + difference + ")");
                            return position;
                        }
                    } else if (difference > FLOAT_EXACT_THRESHOLD || !itemsToSkip.contains(DiffItems.DoublesExact)) {
                        issues.add("Column " + name + " different from the expected set, first difference at row " +
                                position + " encountered " + actualValue + " expected " + expectedValue
                                + " (difference = " + difference + ")");
                        return position;
                    }
                } else if (chunkType == ChunkType.Double) {
                    final double expectedValue = expectedValues.asDoubleChunk().get(ii);
                    final double actualValue = actualValues.asDoubleChunk().get(ii);
                    if (expectedValue == io.deephaven.util.QueryConstants.NULL_DOUBLE
                            || actualValue == io.deephaven.util.QueryConstants.NULL_DOUBLE) {
                        final String actualString = actualValue == io.deephaven.util.QueryConstants.NULL_DOUBLE ? "null"
                                : Double.toString(actualValue);
                        final String expectString =
                                expectedValue == QueryConstants.NULL_DOUBLE ? "null" : Double.toString(expectedValue);
                        issues.add("Column " + name + " different from the expected set, first difference at row " +
                                position + " encountered " + actualString + " expected " + expectString);
                        return position;
                    }
                    final double difference = Math.abs(expectedValue - actualValue);

                    if (itemsToSkip.containsAll(DOUBLES_EXACT_AND_FRACTION)) {
                        final double fracDiff = difference / Math.min(Math.abs(expectedValue), Math.abs(actualValue));
                        // if we are different by more than 0.01%, then we have an error; otherwise it is within bounds
                        if (fracDiff > DOUBLE_EXACT_THRESHOLD) {
                            issues.add("Column " + name + " different from the expected set, first difference at row " +
                                    position + " encountered " + actualValue + " expected " + expectedValue
                                    + " (difference = " + difference + ")");
                            return position;
                        }
                    } else if (difference > DOUBLE_EXACT_THRESHOLD || !itemsToSkip.contains(DiffItems.DoublesExact)) {
                        issues.add("Column " + name + " different from the expected set, first difference at row " +
                                position + " encountered " + actualValue + " expected " + expectedValue
                                + " (difference = " + difference + ")");
                        return position;
                    }
                } else {
                    // noinspection unchecked
                    final String expectedString = ChunkUtils.extractKeyStringFromChunks(new ChunkType[] {chunkType},
                            new Chunk[] {expectedValues}, ii);
                    // noinspection unchecked
                    final String actualString = ChunkUtils.extractKeyStringFromChunks(new ChunkType[] {chunkType},
                            new Chunk[] {actualValues}, ii);

                    issues.add("Column " + name + " different from the expected set, first difference at row " +
                            position + " encountered " + actualString + " expected " + expectedString);
                    return position;
                }
            }
            return -1;
        }

        @Override
        public void close() {
            expectedContext.close();
            actualContext.close();
        }
    }
}
