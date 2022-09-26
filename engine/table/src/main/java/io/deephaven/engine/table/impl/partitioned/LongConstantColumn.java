/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link SelectColumn} implementation to assign a constant {@code long} value.
 */
class LongConstantColumn implements SelectColumn {

    private final String outputColumnName;
    private final long outputValue;

    LongConstantColumn(
            @NotNull final String outputColumnName,
            final long outputValue) {
        this.outputColumnName = outputColumnName;
        this.outputValue = outputValue;
    }

    @Override
    public List<String> initInputs(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        return getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        return getColumns();
    }

    @Override
    public List<String> getColumns() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public ColumnSource<?> getDataView() {
        return new ViewColumnSource<>(long.class, new OutputFormula(), true);
    }

    @Override
    public String getName() {
        return outputColumnName;
    }

    @Override
    public SelectColumn copy() {
        return new LongConstantColumn(outputColumnName, outputValue);
    }

    @Override
    public final List<String> initInputs(@NotNull final Table table) {
        return initInputs(table.getRowSet(), table.getColumnSourceMap());
    }

    @Override
    public final Class<?> getReturnedType() {
        return Table.class;
    }

    @Override
    public final List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public final ColumnSource<?> getLazyView() {
        return getDataView();
    }

    @Override
    public final MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final WritableColumnSource<?> newDestInstance(final long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, Table.class);
    }

    @Override
    public final WritableColumnSource<?> newFlatDestInstance(final long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, Table.class, null);
    }

    @Override
    public final boolean isRetain() {
        return false;
    }

    @Override
    public final boolean disallowRefresh() {
        return false;
    }

    @Override
    public final boolean isStateless() {
        return true;
    }

    private static final class OutputFormulaFillContext implements Formula.FillContext {

        private static final Formula.FillContext INSTANCE = new OutputFormulaFillContext();

        private OutputFormulaFillContext() {}

        @Override
        public void close() {
        }
    }

    private final class OutputFormula extends Formula {

        private OutputFormula() {
            super(null);
        }

        @Override
        public Long get(final long rowKey) {
            return TypeUtils.box(outputValue);
        }

        @Override
        public Long getPrev(final long rowKey) {
            return get(rowKey);
        }

        @Override
        public long getLong(long rowKey) {
            return outputValue;
        }

        @Override
        public long getPrevLong(long rowKey) {
            return getLong(rowKey);
        }

        @Override
        protected ChunkType getChunkType() {
            return ChunkType.Long;
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity) {
            return OutputFormulaFillContext.INSTANCE;
        }

        @Override
        public void fillChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            destination.setSize(rowSequence.intSize());
            destination.asWritableLongChunk().fillWithValue(0, destination.size(), outputValue);
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            fillChunk(context, destination, rowSequence);
        }
    }
}
