package io.deephaven.engine.table.impl.partitioned;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.Formula;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@link SelectColumn} implementation to assign a constant value.
 */
class ConstantColumn<TYPE> extends BaseTableTransformationColumn {

    private final String outputColumnName;
    private final Class<TYPE> outputType;
    private final TYPE outputValue;

    ConstantColumn(
            @NotNull final String outputColumnName,
            @NotNull final Class<TYPE> outputType,
            final TYPE outputValue) {
        this.outputColumnName = outputColumnName;
        this.outputType = outputType;
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
        return new ViewColumnSource<>(Table.class, new OutputFormula(), false, true);
    }

    @Override
    public String getName() {
        return outputColumnName;
    }

    @Override
    public SelectColumn copy() {
        return new ConstantColumn<>(outputColumnName, outputType, outputValue);
    }

    private static final class OutputFormulaFillContext implements Formula.FillContext {

        private static final Formula.FillContext INSTANCE = new OutputFormulaFillContext();

        private OutputFormulaFillContext() {}
    }

    private final class OutputFormula extends Formula {

        private OutputFormula() {
            super(null);
        }

        @Override
        public Object get(final long rowKey) {
            return outputValue;
        }

        @Override
        public Object getPrev(final long rowKey) {
            return outputValue;
        }

        @Override
        protected ChunkType getChunkType() {
            return ChunkType.Object;
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
            destination.asWritableObjectChunk().fillWithValue(0, destination.size(), outputValue);
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
