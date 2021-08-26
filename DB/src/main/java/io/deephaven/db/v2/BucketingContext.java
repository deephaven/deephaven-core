package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.BooleanUtils;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.hashing.ToIntFunctor;
import io.deephaven.db.v2.hashing.ToIntegerCast;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.IntegerSparseArraySource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.regioned.SymbolTableSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.TypeUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.db.tables.select.MatchPair.matchString;

class BucketingContext implements SafeCloseable {
    final int keyColumnCount;
    final boolean useLeftGrouping;
    final boolean useRightGrouping;
    final String listenerDescription;

    final ColumnSource<?>[] leftSources;
    final ColumnSource<?>[] rightSources;
    final ColumnSource<?>[] originalLeftSources;

    ToIntFunctor<Attributes.Values> uniqueFunctor = null;
    boolean uniqueValues = false;
    long maximumUniqueValue = Integer.MAX_VALUE;
    long minimumUniqueValue = Integer.MIN_VALUE;

    BucketingContext(final String listenerPrefix, final QueryTable leftTable, final QueryTable rightTable,
            MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, JoinControl control) {
        final List<String> conflicts = Arrays.stream(columnsToAdd).map(MatchPair::left)
                .filter(cn -> leftTable.getColumnSourceMap().containsKey(cn)).collect(Collectors.toList());
        if (!conflicts.isEmpty()) {
            throw new RuntimeException("Conflicting column names " + conflicts);
        }

        listenerDescription =
                listenerPrefix + "(" + matchString(columnsToMatch) + ", " + matchString(columnsToAdd) + ")";

        leftSources = Arrays.stream(columnsToMatch).map(mp -> leftTable.getColumnSource(mp.leftColumn))
                .toArray(ColumnSource[]::new);
        rightSources = Arrays.stream(columnsToMatch).map(mp -> rightTable.getColumnSource(mp.rightColumn))
                .toArray(ColumnSource[]::new);
        originalLeftSources = Arrays.copyOf(leftSources, leftSources.length);

        keyColumnCount = leftSources.length;
        useLeftGrouping = control.useGrouping(leftTable, leftSources);
        // note that the naturalJoin operation ignores this field, because there is never any point to reading or
        // processing grouping information when we have a single row on the right side. Cross join just doesn't support
        // grouping at all (yuck).
        useRightGrouping = control.useGrouping(rightTable, rightSources);

        for (int ii = 0; ii < keyColumnCount; ++ii) {
            final Class leftType = TypeUtils.getUnboxedTypeIfBoxed(leftSources[ii].getType());
            final Class rightType = TypeUtils.getUnboxedTypeIfBoxed(rightSources[ii].getType());
            if (leftType != rightType) {
                throw new IllegalArgumentException(
                        "Mismatched join types, " + columnsToMatch[ii] + ": " + leftType + " != " + rightType);
            }

            if (leftType == DBDateTime.class) {
                leftSources[ii] = ReinterpretUtilities.dateTimeToLongSource(leftSources[ii]);
                rightSources[ii] = ReinterpretUtilities.dateTimeToLongSource(rightSources[ii]);
            } else if (leftType == boolean.class || leftType == Boolean.class) {
                leftSources[ii] = ReinterpretUtilities.booleanToByteSource(leftSources[ii]);
                rightSources[ii] = ReinterpretUtilities.booleanToByteSource(rightSources[ii]);
                if (leftSources.length == 1) {
                    uniqueValues = true;
                    maximumUniqueValue = BooleanUtils.TRUE_BOOLEAN_AS_BYTE;
                    minimumUniqueValue = BooleanUtils.NULL_BOOLEAN_AS_BYTE;
                    uniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Byte,
                            StaticNaturalJoinStateManager.CHUNK_SIZE, -BooleanUtils.NULL_BOOLEAN_AS_BYTE);
                }
            } else if (leftType == String.class) {
                if (control.considerSymbolTables(leftTable, rightTable, useLeftGrouping, useRightGrouping,
                        leftSources[ii], rightSources[ii])) {
                    final SymbolTableSource leftSymbolTableSource = (SymbolTableSource) leftSources[ii];
                    final SymbolTableSource rightSymbolTableSource = (SymbolTableSource) rightSources[ii];

                    final Table leftSymbolTable = leftSymbolTableSource.getStaticSymbolTable(leftTable.getIndex(),
                            control.useSymbolTableLookupCaching());
                    final Table rightSymbolTable = rightSymbolTableSource.getStaticSymbolTable(rightTable.getIndex(),
                            control.useSymbolTableLookupCaching());

                    if (control.useSymbolTables(leftTable.size(), leftSymbolTable.size(), rightTable.size(),
                            rightSymbolTable.size())) {
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
                                new NaturalJoinHelper.SymbolTableToUniqueIdSource(leftSourceAsLong, leftSymbolMapper);
                        rightSources[ii] =
                                new NaturalJoinHelper.SymbolTableToUniqueIdSource(rightSourceAsLong, rightSymbolMapper);

                        if (leftSources.length == 1) {
                            uniqueValues = true;
                            maximumUniqueValue = symbolTableCombiner.getMaximumIdentifier();
                            minimumUniqueValue = 0;
                            uniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Int,
                                    StaticNaturalJoinStateManager.CHUNK_SIZE, 0);
                        }
                    }
                }
            } else if (leftType == byte.class) {
                if (leftSources.length == 1) {
                    uniqueValues = true;
                    maximumUniqueValue = Byte.MAX_VALUE;
                    minimumUniqueValue = Byte.MIN_VALUE;
                    uniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Byte,
                            StaticNaturalJoinStateManager.CHUNK_SIZE, -Byte.MIN_VALUE);
                }
            } else if (leftType == char.class) {
                if (leftSources.length == 1) {
                    uniqueValues = true;
                    maximumUniqueValue = Character.MAX_VALUE;
                    minimumUniqueValue = Character.MIN_VALUE;
                    uniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Char,
                            StaticNaturalJoinStateManager.CHUNK_SIZE, -Character.MIN_VALUE);
                }
            } else if (leftType == short.class) {
                if (leftSources.length == 1) {
                    uniqueValues = true;
                    maximumUniqueValue = Short.MAX_VALUE;
                    minimumUniqueValue = Short.MIN_VALUE;
                    uniqueFunctor = ToIntegerCast.makeToIntegerCast(ChunkType.Short,
                            StaticNaturalJoinStateManager.CHUNK_SIZE, -Short.MIN_VALUE);
                }
            }
        }
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
