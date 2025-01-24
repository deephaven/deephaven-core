//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.table.impl.JoinControl.BuildParameters.From.*;

@VisibleForTesting
public class JoinControl {
    public enum RedirectionType {
        Contiguous, Sparse, Hash
    }

    public static final int CHUNK_SIZE = 1 << 12;
    private static final int MINIMUM_INITIAL_HASH_SIZE = CHUNK_SIZE;
    public static final long MAX_TABLE_SIZE = 1L << 30;
    private static final double DEFAULT_MAX_LOAD_FACTOR = 0.75;
    private static final double DEFAULT_TARGET_LOAD_FACTOR = 0.70;

    int initialBuildSize() {
        return MINIMUM_INITIAL_HASH_SIZE;
    }

    /**
     * Determine the initial size of the hash table based on the expected number of entries. This is used for static
     * cases where we won't rehash, or when we have a data index with a known size to start us off from. Other cases use
     * {@link #initialBuildSize()}.
     *
     * @param expectedEntries The expected maximum number of entries that will be needed.
     * @return The initial size of the hash table to use
     */
    int tableSize(final long expectedEntries) {
        // Get the target capacity to contain expected entries at target load factor. This is optimizing for time, at
        // the expense of some memory usage.
        final long targetCapacity = (long) Math.ceil(expectedEntries / getTargetLoadFactor());
        final long targetCapacityPowerOf2 = Long.highestOneBit(targetCapacity) << 1;
        // We know the result fits in an int, since MINIMUM_INITIAL_HASH_SIZE and MAX_TABLE_SIZE are positive ints.
        return (int) Math.max(MINIMUM_INITIAL_HASH_SIZE, Math.min(MAX_TABLE_SIZE, targetCapacityPowerOf2));
    }

    double getMaximumLoadFactor() {
        return DEFAULT_MAX_LOAD_FACTOR;
    }

    double getTargetLoadFactor() {
        return DEFAULT_TARGET_LOAD_FACTOR;
    }

    @Nullable
    DataIndex dataIndexToUse(Table table, ColumnSource<?>[] sources) {
        // Configuration property that serves as an escape hatch
        if (!QueryTable.USE_DATA_INDEX_FOR_JOINS) {
            return null;
        }
        final DataIndexer indexer = DataIndexer.existingOf(table.getRowSet());
        return indexer == null ? null
                : LivenessScopeStack.computeEnclosed(
                        // DataIndexer will only give us valid, live data indexes.
                        () -> indexer.getDataIndex(sources),
                        // Ensure that we use an enclosing scope to manage the data index if needed.
                        table::isRefreshing,
                        // Don't keep the data index managed. Joins hold the update graph lock, so the index can't go
                        // stale,
                        // and we'll only use it during instantiation.
                        di -> false);
    }

    static final class BuildParameters {

        enum From {
            LeftInput, LeftDataIndex, RightInput, RightDataIndex
        }

        private final From firstBuildFrom;
        private final int hashTableSize;

        BuildParameters(
                @NotNull final JoinControl.BuildParameters.From firstBuildFrom,
                final int hashTableSize) {
            this.firstBuildFrom = firstBuildFrom;
            this.hashTableSize = hashTableSize;
        }

        From firstBuildFrom() {
            return firstBuildFrom;
        }

        int hashTableSize() {
            return hashTableSize;
        }
    }

    /**
     * Join Control Goals:
     * <ol>
     * <li>Keep the hash table small</li>
     * <li>Avoid rehashing</li>
     * </ol>
     * To meet these goals, we:
     * <dl>
     * <dt>Both sides refreshing</dt>
     * <dd>Build from a data index if available. Size the hash table for the expected highest cardinality side to
     * minimize rehashing. Prefer building from the expected highest cardinality side first, even though it's probably
     * irrelevant.</dd>
     * <dt>One side refreshing</dt>
     * <dd>Always build from the static side. Use a data index if that side has one.</dd>
     * <dt>Both sides static</dt>
     * <dd>Build from the expected lowest cardinality side to avoid creating unnecessary states. Use a data index if
     * available on expected lowest cardinality side.</dd>
     * </dl>
     */
    BuildParameters buildParameters(
            @NotNull final Table leftTable, @Nullable final Table leftDataIndexTable,
            @NotNull final Table rightTable, @Nullable final Table rightDataIndexTable) {

        // If we're going to do our initial build from a data index, choose the hash table size accordingly.
        // Else, choose our default initial size.
        final int leftBuildSize = leftDataIndexTable != null
                ? tableSize(leftDataIndexTable.size())
                : initialBuildSize();
        final int rightBuildSize = rightDataIndexTable != null
                ? tableSize(rightDataIndexTable.size())
                : initialBuildSize();

        final BuildParameters.From firstBuildFrom;
        final int hashTableSize;
        if (leftTable.isRefreshing() && rightTable.isRefreshing()) {
            // Both refreshing: build from largest available data index, or largest table if no indexes
            if (leftDataIndexTable != null && rightDataIndexTable != null) {
                firstBuildFrom = leftDataIndexTable.size() >= rightDataIndexTable.size()
                        ? LeftDataIndex
                        : RightDataIndex;
            } else if (leftDataIndexTable != null) {
                firstBuildFrom = LeftDataIndex;
            } else if (rightDataIndexTable != null) {
                firstBuildFrom = RightDataIndex;
            } else {
                firstBuildFrom = leftTable.size() >= rightTable.size()
                        ? LeftInput
                        : RightInput;
            }

            // We need to hold states from both sides. We'll need a table at least big enough for the largest side.
            hashTableSize = Math.max(leftBuildSize, rightBuildSize);

        } else if (leftTable.isRefreshing()) {
            // Left refreshing, right static: build from right data index if available, else right table
            firstBuildFrom = rightDataIndexTable != null
                    ? RightDataIndex
                    : RightInput;

            // We need to hold states from right, only.
            hashTableSize = rightBuildSize;

        } else if (rightTable.isRefreshing()) {
            // Left static, right refreshing: build from left data index if available, else left table
            firstBuildFrom = leftDataIndexTable != null
                    ? LeftDataIndex
                    : LeftInput;

            // We need to hold states from left, only.
            hashTableSize = leftBuildSize;

        } else {
            // Both static: build from smallest available data index or smallest table; ties go to smallest table
            if (leftDataIndexTable != null && rightDataIndexTable != null) {
                firstBuildFrom = leftDataIndexTable.size() <= rightDataIndexTable.size()
                        ? LeftDataIndex
                        : RightDataIndex;
            } else if (leftDataIndexTable != null) {
                firstBuildFrom = leftDataIndexTable.size() < rightTable.size()
                        ? LeftDataIndex
                        : RightInput;
            } else if (rightDataIndexTable != null) {
                firstBuildFrom = leftTable.size() <= rightDataIndexTable.size()
                        ? LeftInput
                        : RightDataIndex;
            } else {
                firstBuildFrom = leftTable.size() <= rightTable.size()
                        ? LeftInput
                        : RightInput;
            }

            switch (firstBuildFrom) {
                case LeftDataIndex: // Fall through
                case LeftInput:
                    hashTableSize = leftBuildSize;
                    break;
                case RightDataIndex: // Fall through
                case RightInput:
                    hashTableSize = rightBuildSize;
                    break;
                default:
                    throw new IllegalStateException("Unexpected first build from " + firstBuildFrom);
            }
        }
        return new BuildParameters(firstBuildFrom, hashTableSize);
    }

    /**
     * Same as {@link #buildParameters(Table, Table, Table, Table)}, but it's assumed that all RHS rows are unique.
     * That, is the RHS table is treated like its own data index table in some respects.
     */
    BuildParameters buildParametersForUniqueRights(
            @NotNull final Table leftTable, @Nullable Table leftDataIndexTable, @NotNull final Table rightTable) {
        // Treat rightTable as its own data index table, and then fix up the firstBuildFrom to not be RightDataIndex.
        final BuildParameters result = buildParameters(leftTable, leftDataIndexTable, rightTable, rightTable);
        return result.firstBuildFrom() == RightDataIndex
                ? new BuildParameters(RightInput, result.hashTableSize())
                : result;
    }

    boolean considerSymbolTables(
            Table leftTable, Table rightTable,
            boolean useLeftGrouping, boolean useRightGrouping,
            ColumnSource<?> leftSource, ColumnSource<?> rightSource) {
        return !leftTable.isRefreshing() && !useLeftGrouping && leftSource.getType() == String.class
                && !rightTable.isRefreshing() && !useRightGrouping && rightSource.getType() == String.class
                && leftSource instanceof SymbolTableSource && rightSource instanceof SymbolTableSource
                && ((SymbolTableSource<?>) leftSource).hasSymbolTable(leftTable.getRowSet())
                && ((SymbolTableSource<?>) rightSource).hasSymbolTable(rightTable.getRowSet());
    }

    boolean useSymbolTableLookupCaching() {
        return false;
    }

    boolean useSymbolTables(long leftSize, long leftSymbolSize, long rightSize, long rightSymbolSize) {
        final long proposedSymbolSize = Math.min(leftSymbolSize, rightSymbolSize);
        return proposedSymbolSize <= leftSize / 2 || proposedSymbolSize <= rightSize / 2;
    }

    boolean useUniqueTable(boolean uniqueValues, long maximumUniqueValue, long minimumUniqueValue) {
        // we want to have one left over value for "no good" (Integer.MAX_VALUE); and then we need another value to
        // represent that (max - min + 1) is the number of slots required.
        return uniqueValues && (maximumUniqueValue - minimumUniqueValue) < (Integer.MAX_VALUE - 2);
    }

    RedirectionType getRedirectionType(Table leftTable) {
        return getRedirectionType(leftTable, 4.0, true);
    }

    public static RedirectionType getRedirectionType(@NotNull final Table table, final double maximumOverhead,
            final boolean allowSparseRedirection) {
        if (table.isFlat() && table.size() < Integer.MAX_VALUE) {
            if (table.isRefreshing()) {
                return RedirectionType.Sparse;
            } else {
                return RedirectionType.Contiguous;
            }
        } else if (allowSparseRedirection
                && !SparseConstants.sparseStructureExceedsOverhead(table.getRowSet(), maximumOverhead)) {
            // If we are going to use at least a quarter of a sparse array block, then it is a better answer than a
            // hash table for redirection; because the hash table must store both the key and value, and then has a
            // load factor of ~50%. Additionally, the sparse array source will have much faster sets and lookups so is
            // a win, win, win (memory, set, lookup).
            return RedirectionType.Sparse;
        } else {
            return RedirectionType.Hash;
        }
    }

    int rightSsaNodeSize() {
        return 4096;
    }

    int leftSsaNodeSize() {
        return 4096;
    }

    public int rightChunkSize() {
        return 64 * 1024;
    }

    public int leftChunkSize() {
        return rightChunkSize();
    }
}
