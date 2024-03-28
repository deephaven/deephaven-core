//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.*;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FunctionalColumn<S, D> implements SelectColumn {

    @NotNull
    private final String sourceName;
    @NotNull
    private final Class<S> sourceDataType;
    @NotNull
    private final String destName;
    @NotNull
    private final Class<D> destDataType;
    @NotNull
    private final RowKeyAndValueFunction<S, D> function;
    @Nullable
    private final Class<?> componentType;

    private ColumnSource<S> sourceColumnSource;

    @FunctionalInterface
    public interface RowKeyAndValueFunction<S, D> {
        D apply(long rowKey, S value);
    }

    public FunctionalColumn(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull Function<S, D> function) {
        this(sourceName, sourceDataType, destName, destDataType, (l, v) -> function.apply(v));
    }

    public FunctionalColumn(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @Nullable Class<?> componentType,
            @NotNull Function<S, D> function) {
        this(sourceName, sourceDataType, destName, destDataType, componentType, (l, v) -> function.apply(v));
    }

    public FunctionalColumn(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull RowKeyAndValueFunction<S, D> function) {
        this(sourceName, sourceDataType, destName, destDataType, null, function);
    }

    public FunctionalColumn(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @Nullable Class<?> componentType,
            @NotNull RowKeyAndValueFunction<S, D> function) {
        this.sourceName = NameValidator.validateColumnName(sourceName);
        this.sourceDataType = Require.neqNull(sourceDataType, "sourceDataType");
        this.destName = NameValidator.validateColumnName(destName);
        this.destDataType = Require.neqNull(destDataType, "destDataType");
        this.componentType = componentType;
        this.function = function;
        Require.gtZero(destName.length(), "destName.length()");
    }

    @Override
    public String toString() {
        return "function(" + sourceName + ',' + destName + ')';
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        // noinspection unchecked
        final ColumnSource<S> localSourceColumnSource = (ColumnSource<S>) columnsOfInterest.get(sourceName);
        if (localSourceColumnSource == null) {
            throw new NoSuchColumnException(columnsOfInterest.keySet(), sourceName);
        }
        if (!(sourceDataType.isAssignableFrom(localSourceColumnSource.getType()) || sourceDataType
                .isAssignableFrom(io.deephaven.util.type.TypeUtils.getBoxedType(localSourceColumnSource.getType())))) {
            throw new IllegalArgumentException("Source column " + sourceName + " has wrong data type "
                    + localSourceColumnSource.getType() + ", expected " + sourceDataType);
        }
        // noinspection unchecked
        sourceColumnSource = (ColumnSource<S>) columnsOfInterest.get(sourceName);
        return getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        // noinspection unchecked
        final ColumnDefinition<S> sourceColumnDefinition = (ColumnDefinition<S>) columnDefinitionMap.get(sourceName);
        if (sourceColumnDefinition == null) {
            throw new NoSuchColumnException(columnDefinitionMap.keySet(), sourceName);
        }
        if (!(sourceDataType.isAssignableFrom(sourceColumnDefinition.getDataType())
                || sourceDataType.isAssignableFrom(TypeUtils.getBoxedType(sourceColumnDefinition.getDataType())))) {
            throw new IllegalArgumentException("Source column " + sourceName + " has wrong data type "
                    + sourceColumnDefinition.getDataType() + ", expected " + sourceDataType);
        }
        return getColumns();
    }

    @Override
    public Class<?> getReturnedType() {
        return destDataType;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return componentType;
    }

    @Override
    public List<String> getColumns() {
        return List.of(sourceName);
    }

    @Override
    public List<String> getColumnArrays() {
        return List.of();
    }

    @NotNull
    @Override
    public ColumnSource<D> getDataView() {
        return new ViewColumnSource<>(destDataType, componentType, new Formula(null) {

            @Override
            public Object get(final long rowKey) {
                return function.apply(rowKey, sourceColumnSource.get(rowKey));
            }

            @Override
            public Object getPrev(final long rowKey) {
                return function.apply(rowKey, sourceColumnSource.getPrev(rowKey));
            }

            @Override
            public ChunkType getChunkType() {
                return ChunkType.fromElementType(destDataType);
            }

            @Override
            public FillContext makeFillContext(final int chunkCapacity) {
                return new FunctionalColumnFillContext(getChunkType());
            }

            @Override
            public void fillChunk(
                    @NotNull final FillContext fillContext,
                    @NotNull final WritableChunk<? super Values> destination,
                    @NotNull final RowSequence rowSequence) {
                final FunctionalColumnFillContext ctx = (FunctionalColumnFillContext) fillContext;
                ctx.chunkFiller.fillByIndices(this, rowSequence, destination);
            }

            @Override
            public void fillPrevChunk(
                    @NotNull final FillContext fillContext,
                    @NotNull final WritableChunk<? super Values> destination,
                    @NotNull final RowSequence rowSequence) {
                final FunctionalColumnFillContext ctx = (FunctionalColumnFillContext) fillContext;
                ctx.chunkFiller.fillPrevByIndices(this, rowSequence, destination);
            }
        }, false);
    }

    private static class FunctionalColumnFillContext implements Formula.FillContext {

        private final ChunkFiller chunkFiller;

        private FunctionalColumnFillContext(final ChunkType chunkType) {
            chunkFiller = ChunkFiller.forChunkType(chunkType);
        }
    }

    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
        return getDataView();
    }

    @Override
    public String getName() {
        return destName;
    }

    @Override
    public MatchPair getMatchPair() {
        throw new UnsupportedOperationException();
    }

    @Override
    public final WritableColumnSource<?> newDestInstance(final long size) {
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, destDataType);
    }

    @Override
    public final WritableColumnSource<?> newFlatDestInstance(final long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, destDataType, componentType);
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean isStateless() {
        return false;
    }

    @Override
    public FunctionalColumn<S, D> copy() {
        return new FunctionalColumn<>(sourceName, sourceDataType, destName, destDataType, function);
    }
}
