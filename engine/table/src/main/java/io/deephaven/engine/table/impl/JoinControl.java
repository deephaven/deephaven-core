/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    int tableSize(final long initialCapacity) {
        return Math.toIntExact(
                Math.max(MINIMUM_INITIAL_HASH_SIZE,
                        Math.min(MAX_TABLE_SIZE, Long.highestOneBit(initialCapacity) * 2)));
    }

    double getMaximumLoadFactor() {
        return DEFAULT_MAX_LOAD_FACTOR;
    }

    double getTargetLoadFactor() {
        return DEFAULT_TARGET_LOAD_FACTOR;
    }

    @Nullable
    DataIndex dataIndexToUse(Table table, ColumnSource<?>[] sources) {
        final DataIndexer indexer = DataIndexer.existingOf(table.getRowSet());
        return indexer == null ? null : indexer.getDataIndex(sources);
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
     * <dd>Build from a data index if available. Prefer building from the expected highest cardinality side first to try
     * to minimize rehashing.</dd>
     * <dt>One side refreshing</dt>
     * <dd>Always build from the static side. Use a data index if that side has one.</dd>
     * <dt>Both sides static</dt>
     * <dd>Build from the expected lowest cardinality side to avoid creating unnecessary states. Use a data index if
     * available on expected lowest cardinality side.</dd>
     * </dl>
     */
    BuildParameters buildParameters(
            @NotNull final Table leftTable, @Nullable Table leftDataIndexTable,
            @NotNull final Table rightTable, @Nullable Table rightDataIndexTable) {
        final BuildParameters.From firstBuildFrom;
        if (leftTable.isRefreshing()) {
            if (rightTable.isRefreshing()) {
                // Both refreshing: build from largest available data index, or largest table if no indexes
                if (leftDataIndexTable != null) {
                    if (rightDataIndexTable != null) {
                        if (leftDataIndexTable.size() >= rightDataIndexTable.size()) {
                            firstBuildFrom = BuildParameters.From.LeftDataIndex;
                        } else {
                            firstBuildFrom = BuildParameters.From.RightDataIndex;
                        }
                    } else {
                        firstBuildFrom = BuildParameters.From.LeftDataIndex;
                    }
                } else if (rightDataIndexTable != null) {
                    firstBuildFrom = BuildParameters.From.RightDataIndex;
                } else if (leftTable.size() >= rightTable.size()) {
                    firstBuildFrom = BuildParameters.From.LeftInput;
                } else {
                    firstBuildFrom = BuildParameters.From.RightInput;
                }
            } else {
                // Left refreshing, right static: build from right data index if available, else right table
                if (rightDataIndexTable != null) {
                    firstBuildFrom = BuildParameters.From.RightDataIndex;
                } else {
                    firstBuildFrom = BuildParameters.From.RightInput;
                }
            }
        } else if (rightTable.isRefreshing()) {
            // Left static, right refreshing: build from left data index if available, else left table
            if (leftDataIndexTable != null) {
                firstBuildFrom = BuildParameters.From.LeftDataIndex;
            } else {
                firstBuildFrom = BuildParameters.From.LeftInput;
            }
        } else {
            // Both static: build from smallest available data index or smallest table; ties go to smallest table
            if (leftDataIndexTable != null) {
                if (rightDataIndexTable != null) {
                    if (leftDataIndexTable.size() <= rightDataIndexTable.size()) {
                        firstBuildFrom = BuildParameters.From.LeftDataIndex;
                    } else {
                        firstBuildFrom = BuildParameters.From.RightDataIndex;
                    }
                } else if (leftDataIndexTable.size() < rightTable.size()) {
                    firstBuildFrom = BuildParameters.From.LeftDataIndex;
                } else {
                    firstBuildFrom = BuildParameters.From.RightInput;
                }
            } else if (rightDataIndexTable != null) {
                if (leftTable.size() <= rightDataIndexTable.size()) {
                    firstBuildFrom = BuildParameters.From.LeftInput;
                } else {
                    firstBuildFrom = BuildParameters.From.RightDataIndex;
                }
            } else if (leftTable.size() <= rightTable.size()) {
                firstBuildFrom = BuildParameters.From.LeftInput;
            } else {
                firstBuildFrom = BuildParameters.From.RightInput;
            }
        }
        // If we're going to do our initial build from a data index, choose the hash table size accordingly.
        // Else, choose our default initial size.
        final int hashTableSize;
        switch (firstBuildFrom) {
            case LeftDataIndex:
                hashTableSize = tableSize(leftDataIndexTable.size());
                break;
            case RightDataIndex:
                hashTableSize = tableSize(rightDataIndexTable.size());
                break;
            case LeftInput:
            case RightInput:
                hashTableSize = initialBuildSize();
                break;
            default:
                throw new IllegalStateException("Unexpected first build from " + firstBuildFrom);
        }
        return new BuildParameters(firstBuildFrom, hashTableSize);
    }

    /**
     * Same as {@link #buildParameters(Table, Table, Table, Table)}, but it's assumed that all RHS rows are unique.
     * That, is the RHS table is treated like its own data index table in some respects.
     */
    BuildParameters buildParametersForUniqueRights(
            @NotNull final Table leftTable, @Nullable Table leftDataIndexTable, @NotNull final Table rightTable) {
        final BuildParameters result = buildParameters(leftTable, leftDataIndexTable, rightTable, rightTable);
        if (result.firstBuildFrom() == BuildParameters.From.RightDataIndex) {
            return new BuildParameters(BuildParameters.From.RightInput, result.hashTableSize());
        }
        return result;
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
        final long proposedSymbolSize = Math.min(rightSymbolSize, leftSymbolSize);
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
