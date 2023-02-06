/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * {@link SelectColumn} implementation to wrap transformer functions for
 * {@link PartitionedTable#transform(UnaryOperator) transformations}.
 */
public class TableTransformationColumn extends BaseTableTransformationColumn {

    private final String inputOutputColumnName;
    private final Function<Table, Table> transformer;
    private final ExecutionContext executionContext;

    private ColumnSource<Table> inputColumnSource;

    public TableTransformationColumn(
            @NotNull final String inputOutputColumnName,
            final ExecutionContext executionContext,
            @NotNull final Function<Table, Table> transformer) {
        this.inputOutputColumnName = inputOutputColumnName;
        this.executionContext = executionContext;
        this.transformer = transformer;
    }

    @Override
    public List<String> initInputs(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        inputColumnSource = getAndValidateInputColumnSource(inputOutputColumnName, columnsOfInterest);
        return getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        validateInputColumnDefinition(inputOutputColumnName, columnDefinitionMap);
        return getColumns();
    }

    @Override
    public List<String> getColumns() {
        return List.of(inputOutputColumnName);
    }

    @NotNull
    @Override
    public ColumnSource<?> getDataView() {
        return new ViewColumnSource<>(Table.class, new OutputFormula(), true);
    }

    @Override
    public String getName() {
        return inputOutputColumnName;
    }

    @Override
    public SelectColumn copy() {
        return new TableTransformationColumn(inputOutputColumnName, executionContext, transformer);
    }

    private final class OutputFormulaFillContext implements Formula.FillContext {

        private final ChunkSource.GetContext inputColumnSourceGetContext;

        private OutputFormulaFillContext(final int chunkCapacity) {
            inputColumnSourceGetContext = inputColumnSource.makeGetContext(chunkCapacity);
        }

        @Override
        public void close() {
            inputColumnSourceGetContext.close();
        }
    }

    private final class OutputFormula extends Formula {

        private OutputFormula() {
            super(null);
        }

        @Override
        public Object get(final long rowKey) {
            try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
                return transformer.apply(inputColumnSource.get(rowKey));
            }
        }

        @Override
        public Object getPrev(final long rowKey) {
            try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
                return transformer.apply(inputColumnSource.getPrev(rowKey));
            }
        }

        @Override
        protected ChunkType getChunkType() {
            return ChunkType.Object;
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity) {
            return new OutputFormulaFillContext(chunkCapacity);
        }

        @Override
        public void fillChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            final ObjectChunk<Table, ? extends Values> source = inputColumnSource.getChunk(
                    ((OutputFormulaFillContext) context).inputColumnSourceGetContext, rowSequence).asObjectChunk();
            transformAndFill(source, destination);
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            final ObjectChunk<Table, ? extends Values> source = inputColumnSource.getPrevChunk(
                    ((OutputFormulaFillContext) context).inputColumnSourceGetContext, rowSequence).asObjectChunk();
            transformAndFill(source, destination);
        }

        private void transformAndFill(
                @NotNull final ObjectChunk<Table, ? extends Values> source,
                @NotNull final WritableChunk<? super Values> destination) {
            final WritableObjectChunk<Table, ? super Values> typedDestination = destination.asWritableObjectChunk();
            final int size = source.size();
            typedDestination.setSize(size);
            try (final SafeCloseable ignored = executionContext == null ? null : executionContext.open()) {
                for (int ii = 0; ii < size; ++ii) {
                    typedDestination.set(ii, transformer.apply(source.get(ii)));
                }
            }
        }
    }
}
