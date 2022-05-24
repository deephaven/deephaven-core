package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

/**
 * {@link SelectColumn} implementation to wrap transformer functions for
 * {@link PartitionedTable#partitionedTransform(PartitionedTable, BinaryOperator) partitioned transformations}.
 */
class BiTableTransformationColumn extends BaseTableTransformationColumn {

    private final String inputOutputColumnName;
    private final String secondInputColumnName;
    private final BinaryOperator<Table> transformer;

    private ColumnSource<Table> inputColumnSource1;
    private ColumnSource<Table> inputColumnSource2;

    BiTableTransformationColumn(
            @NotNull final String inputOutputColumnName,
            @NotNull final String secondInputColumnName,
            @NotNull final BinaryOperator<Table> transformer) {
        this.inputOutputColumnName = inputOutputColumnName;
        this.secondInputColumnName = secondInputColumnName;
        this.transformer = transformer;
    }

    @Override
    public List<String> initInputs(
            @NotNull final TrackingRowSet rowSet,
            @NotNull final Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        inputColumnSource1 = getAndValidateInputColumnSource(inputOutputColumnName, columnsOfInterest);
        inputColumnSource2 = getAndValidateInputColumnSource(secondInputColumnName, columnsOfInterest);
        return getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        validateInputColumnDefinition(inputOutputColumnName, columnDefinitionMap);
        validateInputColumnDefinition(secondInputColumnName, columnDefinitionMap);
        return getColumns();
    }

    @Override
    public List<String> getColumns() {
        return List.of(inputOutputColumnName, secondInputColumnName);
    }

    @NotNull
    @Override
    public ColumnSource<?> getDataView() {
        return new ViewColumnSource<>(Table.class, new OutputFormula(), false, true);
    }

    @Override
    public String getName() {
        return inputOutputColumnName;
    }

    @Override
    public SelectColumn copy() {
        return new BiTableTransformationColumn(inputOutputColumnName, secondInputColumnName, transformer);
    }

    private final class OutputFormulaFillContext implements Formula.FillContext {

        private final ChunkSource.GetContext inputColumnSource1GetContext;
        private final ChunkSource.GetContext inputColumnSource2GetContext;

        private OutputFormulaFillContext(final int chunkCapacity) {
            inputColumnSource1GetContext = inputColumnSource1.makeGetContext(chunkCapacity);
            inputColumnSource2GetContext = inputColumnSource2.makeGetContext(chunkCapacity);
        }

        @Override
        public void close() {
            inputColumnSource1GetContext.close();
            inputColumnSource2GetContext.close();
        }
    }

    private final class OutputFormula extends Formula {

        private OutputFormula() {
            super(null);
        }

        @Override
        public Object get(final long rowKey) {
            return transformer.apply(inputColumnSource1.get(rowKey), inputColumnSource2.get(rowKey));
        }

        @Override
        public Object getPrev(final long rowKey) {
            return transformer.apply(inputColumnSource1.getPrev(rowKey), inputColumnSource2.getPrev(rowKey));
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
            final ObjectChunk<Table, ? extends Values> source1 = inputColumnSource1.getChunk(
                    ((OutputFormulaFillContext) context).inputColumnSource1GetContext, rowSequence).asObjectChunk();
            final ObjectChunk<Table, ? extends Values> source2 = inputColumnSource2.getChunk(
                    ((OutputFormulaFillContext) context).inputColumnSource2GetContext, rowSequence).asObjectChunk();
            transformAndFill(source1, source2, destination);
        }

        @Override
        public void fillPrevChunk(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            final ObjectChunk<Table, ? extends Values> source1 = inputColumnSource1.getPrevChunk(
                    ((OutputFormulaFillContext) context).inputColumnSource1GetContext, rowSequence).asObjectChunk();
            final ObjectChunk<Table, ? extends Values> source2 = inputColumnSource2.getPrevChunk(
                    ((OutputFormulaFillContext) context).inputColumnSource2GetContext, rowSequence).asObjectChunk();
            transformAndFill(source1, source2, destination);
        }

        private void transformAndFill(
                @NotNull final ObjectChunk<Table, ? extends Values> source1,
                @NotNull final ObjectChunk<Table, ? extends Values> source2,
                @NotNull final WritableChunk<? super Values> destination) {
            final WritableObjectChunk<Table, ? super Values> typedDestination = destination.asWritableObjectChunk();
            final int size = source1.size();
            typedDestination.setSize(size);
            for (int ii = 0; ii < size; ++ii) {
                typedDestination.set(ii, transformer.apply(source1.get(ii), source2.get(ii)));
            }
        }
    }
}
