/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.indexer.RowSetIndexer;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.engine.table.impl.sources.sparse.SparseConstants;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

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

    int tableSizeForRightBuild(Table rightTable) {
        return tableSize(rightTable.size());
    }

    int tableSizeForLeftBuild(Table leftTable) {
        return tableSize(leftTable.size());
    }

    double getMaximumLoadFactor() {
        return DEFAULT_MAX_LOAD_FACTOR;
    }

    double getTargetLoadFactor() {
        return DEFAULT_TARGET_LOAD_FACTOR;
    }

    static boolean useGrouping(Table leftTable, ColumnSource<?>[] leftSources) {
        return !leftTable.isRefreshing() && leftSources.length == 1
                && RowSetIndexer.of(leftTable.getRowSet()).hasGrouping(leftSources[0]);
    }

    boolean buildLeft(QueryTable leftTable, Table rightTable) {
        return !leftTable.isRefreshing() && leftTable.size() <= rightTable.size();
    }

    boolean considerSymbolTables(QueryTable leftTable, @SuppressWarnings("unused") QueryTable rightTable,
            boolean useLeftGrouping, boolean useRightGrouping, ColumnSource<?> leftSource,
            ColumnSource<?> rightSource) {
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
