//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.util.hashing.ToIntFunctor;
import io.deephaven.chunk.util.hashing.ToIntegerCast;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.MatchPair.matchString;

class BucketingContext implements SafeCloseable {

    final String listenerDescription;

    final ColumnSource<?>[] leftSources;
    final ColumnSource<?>[] originalLeftSources;
    final ColumnSource<?>[] rightSources;
    final ColumnSource<?>[] originalRightSources;

    final int keyColumnCount;

    final Table leftDataIndexTable;
    final ColumnSource<?>[] leftDataIndexSources;
    final ColumnSource<?>[] originalLeftDataIndexSources;
    final ColumnSource<RowSet> leftDataIndexRowSetSource;
    final Table rightDataIndexTable;
    final ColumnSource<?>[] rightDataIndexSources;
    final ColumnSource<?>[] originalRightDataIndexSources;
    final ColumnSource<RowSet> rightDataIndexRowSetSource;

    final JoinControl.BuildParameters buildParameters;

    final boolean uniqueValues;
    final long minimumUniqueValue;
    final long maximumUniqueValue;
    final ToIntFunctor<Values> uniqueFunctor;

    BucketingContext(
            @NotNull final String listenerPrefix,
            @NotNull final QueryTable leftTable,
            @NotNull final QueryTable rightTable,
            @NotNull final MatchPair[] columnsToMatch,
            @NotNull final MatchPair[] columnsToAdd,
            @NotNull final JoinControl control,
            final boolean uniqueRightValues,
            final boolean useDataIndexes) {

        final Set<String> leftColumnNames = leftTable.getDefinition().getColumnNameSet();
        final List<String> conflicts = Arrays.stream(columnsToAdd)
                .map(MatchPair::leftColumn)
                .filter(leftColumnNames::contains)
                .collect(Collectors.toList());
        if (!conflicts.isEmpty()) {
            throw new RuntimeException("Conflicting column names " + conflicts);
        }

        listenerDescription =
                listenerPrefix + "(" + matchString(columnsToMatch) + ", " + matchString(columnsToAdd) + ")";

        leftSources = Arrays.stream(columnsToMatch)
                .map(mp -> leftTable.getColumnSource(mp.leftColumn))
                .toArray(ColumnSource[]::new);
        originalLeftSources = Arrays.copyOf(leftSources, leftSources.length);
        rightSources = Arrays.stream(columnsToMatch)
                .map(mp -> rightTable.getColumnSource(mp.rightColumn))
                .toArray(ColumnSource[]::new);
        originalRightSources = Arrays.copyOf(rightSources, rightSources.length);

        keyColumnCount = leftSources.length;

        final DataIndex leftDataIndex = useDataIndexes ? control.dataIndexToUse(leftTable, originalLeftSources) : null;
        leftDataIndexTable = leftDataIndex == null ? null : leftDataIndex.table();
        final DataIndex rightDataIndex;

        if (uniqueRightValues) {
            rightDataIndex = null;
            rightDataIndexTable = null;
            buildParameters = control.buildParametersForUniqueRights(leftTable, leftDataIndexTable, rightTable);
        } else {
            rightDataIndex = useDataIndexes ? control.dataIndexToUse(rightTable, originalRightSources) : null;
            rightDataIndexTable = rightDataIndex == null ? null : rightDataIndex.table();
            buildParameters = control.buildParameters(leftTable, leftDataIndexTable, rightTable, rightDataIndexTable);
        }

        if (leftDataIndexTable == null) {
            leftDataIndexSources = null;
            originalLeftDataIndexSources = null;
            leftDataIndexRowSetSource = null;
        } else {
            leftDataIndexSources = Arrays.stream(columnsToMatch)
                    .map(mp -> leftDataIndexTable.getColumnSource(mp.leftColumn))
                    .toArray(ColumnSource[]::new);
            originalLeftDataIndexSources = Arrays.copyOf(leftDataIndexSources, leftDataIndexSources.length);
            leftDataIndexRowSetSource = leftDataIndex.rowSetColumn();
        }

        if (rightDataIndexTable == null) {
            rightDataIndexSources = null;
            originalRightDataIndexSources = null;
            rightDataIndexRowSetSource = null;
        } else {
            rightDataIndexSources = Arrays.stream(columnsToMatch)
                    .map(mp -> rightDataIndexTable.getColumnSource(mp.rightColumn))
                    .toArray(ColumnSource[]::new);
            originalRightDataIndexSources = Arrays.copyOf(rightDataIndexSources, rightDataIndexSources.length);
            rightDataIndexRowSetSource = rightDataIndex.rowSetColumn();
        }

        boolean localUniqueValues = false;
        long localMinimumUniqueValue = Integer.MIN_VALUE;
        long localMaximumUniqueValue = Integer.MAX_VALUE;
        ToIntFunctor<Values> localUniqueFunctor = null;

        for (int ii = 0; ii < keyColumnCount; ++ii) {
            final Class<?> leftType = TypeUtils.getUnboxedTypeIfBoxed(leftSources[ii].getType());
            final Class<?> rightType = TypeUtils.getUnboxedTypeIfBoxed(rightSources[ii].getType());
            if (leftType != rightType) {
                throw new IllegalArgumentException(
                        "Mismatched join types, " + columnsToMatch[ii] + ": " + leftType + " != " + rightType);
            }

            if (leftType == Instant.class) {
                // noinspection unchecked
                leftSources[ii] = ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) leftSources[ii]);
                if (leftDataIndexTable != null) {
                    // noinspection unchecked
                    leftDataIndexSources[ii] =
                            ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) leftDataIndexSources[ii]);
                }
                // noinspection unchecked
                rightSources[ii] = ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) rightSources[ii]);
                if (rightDataIndexTable != null) {
                    // noinspection unchecked
                    rightDataIndexSources[ii] =
                            ReinterpretUtils.instantToLongSource((ColumnSource<Instant>) rightDataIndexSources[ii]);
                }
            } else if (leftType == boolean.class || leftType == Boolean.class) {
                // noinspection unchecked
                leftSources[ii] = ReinterpretUtils.booleanToByteSource((ColumnSource<Boolean>) leftSources[ii]);
                if (leftDataIndexTable != null) {
                    // noinspection unchecked
                    leftDataIndexSources[ii] =
                            ReinterpretUtils.booleanToByteSource((ColumnSource<Boolean>) leftDataIndexSources[ii]);
                }
                // noinspection unchecked
                rightSources[ii] = ReinterpretUtils.booleanToByteSource((ColumnSource<Boolean>) rightSources[ii]);
                if (rightDataIndexTable != null) {
                    // noinspection unchecked
                    rightDataIndexSources[ii] =
                            ReinterpretUtils.booleanToByteSource((ColumnSource<Boolean>) rightDataIndexSources[ii]);
                }
                if (leftSources.length == 1) {
                    localUniqueValues = true;
                    localMinimumUniqueValue = BooleanUtils.NULL_BOOLEAN_AS_BYTE;
                    localMaximumUniqueValue = BooleanUtils.TRUE_BOOLEAN_AS_BYTE;
                    localUniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Byte,
                            JoinControl.CHUNK_SIZE, -BooleanUtils.NULL_BOOLEAN_AS_BYTE);
                }
            } else if (leftType == String.class) {
                if (control.considerSymbolTables(leftTable, rightTable,
                        leftDataIndexTable != null, rightDataIndexTable != null,
                        leftSources[ii], rightSources[ii])) {
                    // If we're considering symbol tables, we cannot be using data indexes. Firstly, there's no point,
                    // since we expect that using the data indexes will be faster. Secondly, if one side is refreshing,
                    // we don't have a good way to ensure that the data index table on that side uses the same symbol
                    // keys as the original table.
                    Assert.eqNull(leftDataIndexTable, "leftDataIndexTable");
                    Assert.eqNull(rightDataIndexTable, "rightDataIndexTable");

                    final SymbolTableSource<?> leftSymbolTableSource = (SymbolTableSource<?>) leftSources[ii];
                    final SymbolTableSource<?> rightSymbolTableSource = (SymbolTableSource<?>) rightSources[ii];

                    final Table leftSymbolTable = leftSymbolTableSource.getStaticSymbolTable(leftTable.getRowSet(),
                            control.useSymbolTableLookupCaching());
                    final Table rightSymbolTable = rightSymbolTableSource.getStaticSymbolTable(rightTable.getRowSet(),
                            control.useSymbolTableLookupCaching());

                    if (control.useSymbolTables(
                            leftTable.size(), leftSymbolTable.size(),
                            rightTable.size(), rightSymbolTable.size())) {
                        final SymbolTableCombiner symbolTableCombiner =
                                new SymbolTableCombiner(new ColumnSource[] {leftSources[ii]}, SymbolTableCombiner
                                        .hashTableSize(Math.max(leftSymbolTable.size(), rightSymbolTable.size())));

                        final IntegerSparseArraySource leftSymbolMapper = new IntegerSparseArraySource();
                        final IntegerSparseArraySource rightSymbolMapper = new IntegerSparseArraySource();

                        if (leftSymbolTable.size() < rightSymbolTable.size()) {
                            symbolTableCombiner.addSymbols(leftSymbolTable, leftSymbolMapper);
                            symbolTableCombiner.lookupSymbols(rightSymbolTable, rightSymbolMapper, Integer.MAX_VALUE);
                        } else {
                            symbolTableCombiner.addSymbols(rightSymbolTable, rightSymbolMapper);
                            symbolTableCombiner.lookupSymbols(leftSymbolTable, leftSymbolMapper, Integer.MAX_VALUE);
                        }

                        final ColumnSource<Long> leftSourceAsLong = leftSources[ii].reinterpret(long.class);
                        final ColumnSource<Long> rightSourceAsLong = rightSources[ii].reinterpret(long.class);

                        leftSources[ii] =
                                new SymbolTableToUniqueIdSource(leftSourceAsLong, leftSymbolMapper);
                        rightSources[ii] =
                                new SymbolTableToUniqueIdSource(rightSourceAsLong, rightSymbolMapper);

                        if (leftSources.length == 1) {
                            localUniqueValues = true;
                            localMinimumUniqueValue = 0;
                            localMaximumUniqueValue = symbolTableCombiner.getMaximumIdentifier();
                            localUniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Int,
                                    JoinControl.CHUNK_SIZE, 0);
                        }
                    }
                }
            } else if (leftType == byte.class) {
                if (leftSources.length == 1) {
                    localUniqueValues = true;
                    localMinimumUniqueValue = Byte.MIN_VALUE;
                    localMaximumUniqueValue = Byte.MAX_VALUE;
                    localUniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Byte,
                            JoinControl.CHUNK_SIZE, -Byte.MIN_VALUE);
                }
            } else if (leftType == char.class) {
                if (leftSources.length == 1) {
                    localUniqueValues = true;
                    localMinimumUniqueValue = Character.MIN_VALUE;
                    localMaximumUniqueValue = Character.MAX_VALUE;
                    localUniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Char,
                            JoinControl.CHUNK_SIZE, -Character.MIN_VALUE);
                }
            } else if (leftType == short.class) {
                if (leftSources.length == 1) {
                    localUniqueValues = true;
                    localMinimumUniqueValue = Short.MIN_VALUE;
                    localMaximumUniqueValue = Short.MAX_VALUE;
                    localUniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Short,
                            JoinControl.CHUNK_SIZE, -Short.MIN_VALUE);
                }
            }
        }
        this.uniqueValues = localUniqueValues;
        this.minimumUniqueValue = localMinimumUniqueValue;
        this.maximumUniqueValue = localMaximumUniqueValue;
        this.uniqueFunctor = localUniqueFunctor;
    }

    @Override
    public void close() {
        if (uniqueFunctor != null) {
            uniqueFunctor.close();
        }
    }

    int uniqueValuesRange() {
        return LongSizedDataStructure.intSize("int cast", maximumUniqueValue - minimumUniqueValue + 1);
    }
}
