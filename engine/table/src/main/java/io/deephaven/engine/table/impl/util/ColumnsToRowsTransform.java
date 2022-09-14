/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sort.permute.PermuteKernel;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.sources.BitShiftingColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Convert value columns into labeled rows.
 *
 * <p>
 * There are times when you have a wide table, that is better displayed to the user as a narrow table with additional
 * rows. For example, you might have a table with columns for "Bid", "Ask" and "Last", which you may prefer to have
 * three rows, one each for Bid, Ask, and Last with a label for each symbol.
 * </p>
 *
 * <p>
 * The same can be accomplished by calling
 * <code>.update("Label=new String[]{`Bid`, `Ask`, `Last`}", "Value=new double[]{Bid, Ask, Last}").ungroup()</code>, but
 * the creation of arrays in the update statement introduces additional overhead and garbage creation to the query
 * execution.
 * </p>
 *
 * <p>
 * You may have only a single label column, but you may define multiple output value columns, all of which must have the
 * same number of source columns.
 * </p>
 *
 * <p>
 * For each output value column, all of the constituent input columns columns must have the same type. If the types are
 * different, then an IllegalArgumentException is thrown.
 * </p>
 *
 * <p>
 * For example, when calling @{code ColumnsToRowsTransform.columnsToRows(inTable, "Name", new String[]{"IV", "DV"}, new
 * String[]{"Apple", "Banana", "Canteloupe"}, new String[][]{new String[]{"Val1", "Val2", "Val3"}, new String[]{"D1",
 * "D2", "D3"}});}, on this table:
 *
 * <pre>
 *        Sym|      Val1|                  D1|                  D2|      Val2|      Val3|                  D3
 * ----------+----------+--------------------+--------------------+----------+----------+--------------------
 * AAPL      |         1|                 7.7|                 9.9|         3|         5|               11.11
 * SPY       |         2|                 8.8|                10.1|         4|         6|               12.12
 * </pre>
 *
 * The expected output is:
 *
 * <pre>
 *        Sym|      Name|        IV|                  DV
 * ----------+----------+----------+--------------------
 * AAPL      |Apple     |         1|                 7.7
 * AAPL      |Banana    |         3|                 9.9
 * AAPL      |Canteloupe|         5|               11.11
 * SPY       |Apple     |         2|                 8.8
 * SPY       |Banana    |         4|                10.1
 * SPY       |Canteloupe|         6|               12.12
 * </pre>
 * </p>
 */
public class ColumnsToRowsTransform {
    /**
     * Convert value columns to labeled rows.
     *
     * @param source the table with multiple value columns
     * @param labelColumn the output column name for the label column
     * @param valueColumn the output column name for the value column
     * @param transposeColumns the names of the columns to transpose, the label value is the name of the column
     * @return the transformed table
     */
    public static Table columnsToRows(final Table source, final String labelColumn, final String valueColumn,
            final String... transposeColumns) {
        return columnsToRows(source, labelColumn, valueColumn, transposeColumns, transposeColumns);
    }

    /**
     * Convert value columns to labeled rows.
     *
     * @param source the table with multiple value columns
     * @param labelColumn the output column name for the label column
     * @param valueColumn the output column name for the value column
     * @param labels the labels for the transposed columns, must be parallel to transposeColumns
     * @param transposeColumns the input column names to transpose, must be parallel to labels
     * @return the transformed table
     */
    public static Table columnsToRows(final Table source, final String labelColumn, final String valueColumn,
            final String[] labels, final String[] transposeColumns) {
        return columnsToRows(source, labelColumn, new String[] {valueColumn}, labels,
                new String[][] {transposeColumns});
    }

    /**
     * Convert value columns to labeled rows.
     *
     * @param source the table with multiple value columns
     * @param labelColumn the output column name for the label column
     * @param valueColumns the output column names for the value columns
     * @param labels the labels for the transposed columns, must be parallel to each element of transposeColumns
     * @param transposeColumns an array parallel to valueColumns; each element is in turn an array of input column names
     *        that are constituents for the output column. The input columns within each element must be the same type,
     *        and the cardinality much match labels.
     * @return the transformed table
     */
    public static Table columnsToRows(final Table source, final String labelColumn, final String[] valueColumns,
            final String[] labels, final String[][] transposeColumns) {
        final QueryTable querySource = (QueryTable) source.coalesce();
        if (valueColumns.length == 0) {
            throw new IllegalArgumentException("No columns to transpose defined!");
        }
        if (valueColumns.length != transposeColumns.length) {
            throw new IllegalArgumentException("Inconsistent transpose column definition, " + valueColumns.length
                    + " names defined, " + transposeColumns.length + " columns defined.");
        }
        for (int cc = 0; cc < transposeColumns.length; ++cc) {
            if (labels.length != transposeColumns[cc].length) {
                throw new IllegalArgumentException(labels.length + " labels defined, but " + transposeColumns[cc].length
                        + " transpose columns defined for " + valueColumns[cc] + ".");
            }
        }

        final int fanout = labels.length;
        final int fanoutPow2 = fanout == 1 ? fanout : Integer.highestOneBit(fanout - 1) << 1;

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();

        final Set<String> allTransposeSet = new HashSet<>();
        final List<Set<String>> transposeSet = new ArrayList<>(transposeColumns.length);
        for (final String[] transposeColumn : transposeColumns) {
            final List<String> tc = Arrays.asList(transposeColumn);
            transposeSet.add(new HashSet<>(tc));
            allTransposeSet.addAll(tc);
        }
        final List<String> expandSet = new ArrayList<>();

        final int bits = 64 - Long.numberOfLeadingZeros(fanout - 1);
        final CrossJoinShiftState crossJoinShiftState = bits > 0 ? new CrossJoinShiftState(bits) : null;
        final Class<?>[] valueTypes = new Class[transposeColumns.length];
        final String[] typeSourceName = new String[transposeColumns.length];
        final ColumnSource<?>[][] sourcesToTranspose = new ColumnSource[transposeColumns.length][labels.length];
        for (int cc = 0; cc < transposeColumns.length; ++cc) {
            for (int dd = 0; dd < transposeColumns[cc].length; ++dd) {
                sourcesToTranspose[cc][dd] = querySource.getColumnSource(transposeColumns[cc][dd]);
            }
        }

        querySource.getColumnSourceMap().forEach((name, cs) -> {
            if (allTransposeSet.contains(name)) {
                for (int cc = 0; cc < transposeColumns.length; ++cc) {
                    if (transposeSet.get(cc).contains(name)) {
                        if (valueTypes[cc] == null) {
                            valueTypes[cc] = cs.getType();
                            typeSourceName[cc] = name;
                        } else {
                            if (valueTypes[cc] != cs.getType()) {
                                throw new IllegalArgumentException("Incompatible transpose types " + typeSourceName[cc]
                                        + " is " + valueTypes[cc] + ", " + name + " is " + cs.getType());
                            }
                        }
                        return;
                    }
                }
                throw new IllegalStateException("Found a transpose column not in one of the sets!");
            }
            expandSet.add(name);
            if (crossJoinShiftState != null) {
                resultMap.put(name, new BitShiftingColumnSource<>(crossJoinShiftState, cs));
            } else {
                resultMap.put(name, cs);
            }
        });
        resultMap.put(labelColumn, new LabelColumnSource(bits, labels));
        if (bits == 0) {
            for (int cc = 0; cc < valueColumns.length; cc++) {
                resultMap.put(valueColumns[cc], sourcesToTranspose[cc][0]);
            }
        } else {
            for (int cc = 0; cc < valueColumns.length; cc++) {
                resultMap.put(valueColumns[cc],
                        new TransposedColumnSource<>(valueTypes[cc], bits, sourcesToTranspose[cc]));
            }
        }

        final TrackingWritableRowSet resultRowSet =
                transformIndex(querySource.getRowSet(), fanout, fanoutPow2).toTracking();

        final QueryTable result = new QueryTable(resultRowSet, resultMap);

        if (querySource.isRefreshing()) {
            final int sourceColumnCount = querySource.getColumnSourceMap().size();
            final ModifiedColumnSet[] resultColumnSets = new ModifiedColumnSet[sourceColumnCount];
            final String[] sourceColumns = new String[sourceColumnCount];
            final MutableInt columnIndex = new MutableInt();
            final ModifiedColumnSet modifyAll = querySource
                    .newModifiedColumnSet(expandSet.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            final ModifiedColumnSet[] modifyOneRow = new ModifiedColumnSet[labels.length];
            // noinspection unchecked
            final List<String>[] sourcesForRow = new ArrayList[labels.length];
            final int[] transposeIndex = new int[transposeColumns.length];
            for (int cc = 0; cc < labels.length; ++cc) {
                sourcesForRow[cc] = new ArrayList<>();
            }

            querySource.getColumnSourceMap().forEach((name, cs) -> {
                sourceColumns[columnIndex.intValue()] = name;
                if (allTransposeSet.contains(name)) {
                    for (int cc = 0; cc < transposeSet.size(); ++cc) {
                        if (transposeSet.get(cc).contains(name)) {
                            resultColumnSets[columnIndex.intValue()] = result.newModifiedColumnSet(valueColumns[cc]);
                            sourcesForRow[transposeIndex[cc]++].add(name);
                        }
                    }
                } else {
                    resultColumnSets[columnIndex.intValue()] = result.newModifiedColumnSet(name);
                }
                columnIndex.increment();
            });

            for (int cc = 0; cc < labels.length; ++cc) {
                modifyOneRow[cc] = querySource
                        .newModifiedColumnSet(sourcesForRow[cc].toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
            }

            final ModifiedColumnSet.Transformer transformer =
                    querySource.newModifiedColumnSetTransformer(sourceColumns, resultColumnSets);
            querySource.listenForUpdates(new BaseTable.ListenerImpl("columnsToRows(" + labelColumn + ", "
                    + Arrays.toString(valueColumns) + ", " + Arrays.deepToString(transposeColumns) + ")", querySource,
                    result) {
                @Override
                public void onUpdate(final TableUpdate upstream) {
                    final TableUpdateImpl downstream = new TableUpdateImpl();
                    downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
                    downstream.added = transformIndex(upstream.added(), fanout, fanoutPow2);
                    downstream.removed = transformIndex(upstream.removed(), fanout, fanoutPow2);

                    if (upstream.modified().isNonempty()) {
                        final boolean expandModified = upstream.modifiedColumnSet().containsAny(modifyAll);
                        if (expandModified) {
                            // all rows are modified, because there is an expanded column modified
                            downstream.modified = transformIndex(upstream.modified(), fanout, fanoutPow2);
                        } else {
                            // we should determine modifications based on the value changes
                            final boolean[] rowModified = new boolean[modifyOneRow.length];
                            boolean allTrue = true;
                            int maxModified = 0;
                            for (int ii = 0; ii < rowModified.length; ++ii) {
                                final boolean modified = upstream.modifiedColumnSet().containsAny(modifyOneRow[ii]);
                                rowModified[ii] = modified;
                                if (modified) {
                                    maxModified = ii;
                                } else {
                                    allTrue = false;
                                }
                            }
                            if (allTrue) {
                                downstream.modified = transformIndex(upstream.modified(), fanout, fanoutPow2);
                            } else {
                                downstream.modified =
                                        transformIndex(upstream.modified(), fanoutPow2, rowModified, maxModified);
                            }
                        }
                    } else {
                        downstream.modified = RowSetFactory.empty();
                    }

                    resultRowSet.remove(downstream.removed());

                    if (upstream.shifted().empty()) {
                        downstream.shifted = RowSetShiftData.EMPTY;
                    } else {
                        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
                        final int shiftCount = upstream.shifted().size();
                        for (int ii = 0; ii < shiftCount; ++ii) {
                            final long beginRange = upstream.shifted().getBeginRange(ii) * fanoutPow2;
                            final long endRange = upstream.shifted().getEndRange(ii) * fanoutPow2 + fanoutPow2 - 1;
                            final long delta = upstream.shifted().getShiftDelta(ii) * fanoutPow2;

                            shiftBuilder.shiftRange(beginRange, endRange, delta);
                        }

                        downstream.shifted = shiftBuilder.build();
                        downstream.shifted().apply(resultRowSet);
                    }


                    resultRowSet.insert(downstream.added());

                    transformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet());
                    result.notifyListeners(downstream);
                }
            });
        }

        return result;
    }

    private static WritableRowSet transformIndex(final RowSet rowSet, final int fanout, final int fanoutPow2) {
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        rowSet.forAllRowKeys(idx -> sequentialBuilder.appendRange(idx * fanoutPow2, idx * fanoutPow2 + fanout - 1));
        return sequentialBuilder.build();
    }

    private static RowSet transformIndex(final RowSet rowSet, final int fanoutPow2, final boolean[] rowModified,
            final int maxModified) {
        final RowSetBuilderSequential sequentialBuilder = RowSetFactory.builderSequential();
        rowSet.forAllRowKeys(idx -> {
            for (int ii = 0; ii <= maxModified; ++ii) {
                if (rowModified[ii]) {
                    sequentialBuilder.appendKey(idx * fanoutPow2 + ii);
                }
            }
        });
        return sequentialBuilder.build();
    }

    private static class LabelColumnSource extends AbstractColumnSource.DefaultedImmutable<String> {
        private final long mask;
        private final String[] labels;

        private LabelColumnSource(final int shiftBits, final String[] labels) {
            super(String.class);
            this.mask = (1L << shiftBits) - 1;
            this.labels = Arrays.copyOf(labels, labels.length);
        }

        @Override
        public String get(final long rowKey) {
            return getLabel(rowKey);
        }

        private String getLabel(final long index) {
            return labels[(int) (index & mask)];
        }

        @Override
        public void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            final MutableInt outputPosition = new MutableInt();
            final WritableObjectChunk<String, ?> objectChunk = destination.asWritableObjectChunk();
            destination.setSize(rowSequence.intSize());
            rowSequence.forAllRowKeys(idx -> {
                objectChunk.set(outputPosition.intValue(), getLabel(idx));
                outputPosition.increment();
            });
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            fillChunk(context, destination, rowSequence);
        }
    }

    private static class TransposedColumnSource<T> extends AbstractColumnSource<T> {
        private final int bits;
        private final long mask;
        private final boolean isImmutable;
        private final ColumnSource<?>[] transposeColumns;

        private TransposedColumnSource(final Class<T> valueType, final int bits,
                final ColumnSource<?>[] transposeColumns) {
            super(valueType);
            this.bits = bits;
            this.mask = (1L << bits) - 1;
            this.transposeColumns = transposeColumns;
            this.isImmutable = Arrays.stream(transposeColumns).allMatch(ColumnSource::isImmutable);
        }

        @Override
        public T get(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            // noinspection unchecked
            return (T) transposeColumns[sourceColumn].get(sourceIndex);
        }

        @Override
        public Boolean getBoolean(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getBoolean(sourceIndex);
        }

        @Override
        public byte getByte(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getByte(sourceIndex);
        }

        @Override
        public char getChar(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getChar(sourceIndex);
        }

        @Override
        public double getDouble(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getDouble(sourceIndex);
        }

        @Override
        public float getFloat(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getFloat(sourceIndex);
        }

        @Override
        public int getInt(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getInt(sourceIndex);
        }

        @Override
        public long getLong(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getLong(sourceIndex);
        }

        @Override
        public short getShort(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getShort(sourceIndex);
        }

        @Override
        public T getPrev(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            // noinspection unchecked
            return (T) transposeColumns[sourceColumn].getPrev(sourceIndex);
        }

        @Override
        public Boolean getPrevBoolean(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevBoolean(sourceIndex);
        }

        @Override
        public byte getPrevByte(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevByte(sourceIndex);
        }

        @Override
        public char getPrevChar(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevChar(sourceIndex);
        }

        @Override
        public double getPrevDouble(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevDouble(sourceIndex);
        }

        @Override
        public float getPrevFloat(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevFloat(sourceIndex);
        }

        @Override
        public int getPrevInt(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevInt(sourceIndex);
        }

        @Override
        public long getPrevLong(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevLong(sourceIndex);
        }

        @Override
        public short getPrevShort(final long rowKey) {
            final int sourceColumn = (int) (rowKey & mask);
            final long sourceIndex = rowKey >> bits;
            return transposeColumns[sourceColumn].getPrevShort(sourceIndex);
        }

        @Override
        public boolean isImmutable() {
            return isImmutable;
        }

        private class TransposeFillContext implements FillContext {
            final WritableChunk<? super Values> tempValues;
            final FillContext[] innerContexts;
            final WritableLongChunk<OrderedRowKeys>[] innerKeys;
            final WritableIntChunk<ChunkPositions>[] outputPositions;
            final PermuteKernel permuteKernel;

            private TransposeFillContext(final int chunkCapacity) {
                tempValues = getChunkType().makeWritableChunk(chunkCapacity);
                permuteKernel = PermuteKernel.makePermuteKernel(getChunkType());
                innerContexts = Arrays.stream(transposeColumns).map(tc -> tc.makeFillContext(chunkCapacity))
                        .toArray(FillContext[]::new);
                // noinspection unchecked
                innerKeys = new WritableLongChunk[transposeColumns.length];
                // noinspection unchecked
                outputPositions = new WritableIntChunk[transposeColumns.length];
                for (int ii = 0; ii < transposeColumns.length; ++ii) {
                    innerKeys[ii] = WritableLongChunk.makeWritableChunk(chunkCapacity);
                    outputPositions[ii] = WritableIntChunk.makeWritableChunk(chunkCapacity);
                }
            }

            @Override
            public void close() {
                tempValues.close();
                Arrays.stream(innerContexts).forEach(Context::close);
                Arrays.stream(innerKeys).forEach(WritableLongChunk::close);
                Arrays.stream(outputPositions).forEach(WritableIntChunk::close);
            }
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            return new TransposeFillContext(chunkCapacity);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            // noinspection unchecked
            final TransposeFillContext transposeFillContext = (TransposeFillContext) context;
            updateContext(transposeFillContext, rowSequence);
            doFillAndPermute(destination, transposeFillContext, false, rowSequence.intSize());
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            // noinspection unchecked
            final TransposeFillContext transposeFillContext = (TransposeFillContext) context;
            updateContext(transposeFillContext, rowSequence);
            doFillAndPermute(destination, transposeFillContext, true, rowSequence.intSize());
        }

        private void updateContext(@NotNull final TransposeFillContext context,
                @NotNull final RowSequence rowSequence) {
            for (int ii = 0; ii < transposeColumns.length; ++ii) {
                context.innerKeys[ii].setSize(0);
                context.outputPositions[ii].setSize(0);
            }
            final MutableInt outputPosition = new MutableInt();
            rowSequence.forAllRowKeys(idx -> {
                final int sourceColumn = (int) (idx & mask);
                final long sourceIndex = idx >> bits;
                context.outputPositions[sourceColumn].add(outputPosition.intValue());
                outputPosition.increment();
                context.innerKeys[sourceColumn].add(sourceIndex);
            });
        }

        private void doFillAndPermute(@NotNull final WritableChunk<? super Values> destination,
                final TransposeFillContext transposeFillContext, final boolean usePrev, final int originalSize) {
            for (int ii = 0; ii < transposeColumns.length; ++ii) {
                if (transposeFillContext.innerKeys[ii].size() == 0) {
                    continue;
                }
                final boolean isComplete = transposeFillContext.innerKeys[ii].size() == originalSize;
                final WritableChunk<? super Values> tempDest =
                        isComplete ? destination : transposeFillContext.tempValues;
                try (final RowSequence innerOk =
                        RowSequenceFactory.wrapRowKeysChunkAsRowSequence(transposeFillContext.innerKeys[ii])) {
                    if (usePrev) {
                        transposeColumns[ii].fillPrevChunk(transposeFillContext.innerContexts[ii], tempDest, innerOk);
                    } else {
                        transposeColumns[ii].fillChunk(transposeFillContext.innerContexts[ii], tempDest, innerOk);
                    }
                }
                if (isComplete) {
                    return;
                }
                destination.setSize(originalSize);
                // noinspection unchecked,rawtypes
                transposeFillContext.permuteKernel.permute((WritableChunk) transposeFillContext.tempValues,
                        transposeFillContext.outputPositions[ii], destination);
            }
        }

        @Override
        public boolean isStateless() {
            return Arrays.stream(transposeColumns).allMatch(ColumnSource::isStateless);
        }
    }
}
