/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.*;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
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
    private final BiFunction<Long, S, D> function;
    @NotNull
    private final Class<?> componentType;

    private ColumnSource<S> sourceColumnSource;

    public FunctionalColumn(@NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull Function<S, D> function) {
        this(sourceName, sourceDataType, destName, destDataType, (l, v) -> function.apply(v));
    }

    public FunctionalColumn(@NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull Class<?> componentType,
            @NotNull Function<S, D> function) {
        this(sourceName, sourceDataType, destName, destDataType, componentType, (l, v) -> function.apply(v));
    }

    public FunctionalColumn(@NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull BiFunction<Long, S, D> function) {
        this(sourceName, sourceDataType, destName, destDataType, Object.class, function);
    }

    public FunctionalColumn(@NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull Class<?> componentType,
            @NotNull BiFunction<Long, S, D> function) {
        this.sourceName = NameValidator.validateColumnName(sourceName);
        this.sourceDataType = Require.neqNull(sourceDataType, "sourceDataType");
        this.destName = NameValidator.validateColumnName(destName);
        this.destDataType = Require.neqNull(destDataType, "destDataType");
        this.componentType = Require.neqNull(componentType, "componentType");
        this.function = function;
        Require.gtZero(destName.length(), "destName.length()");
    }

    @Override
    public String toString() {
        return "function(" + sourceName + ',' + destName + ')';
    }

    @Override
    public List<String> initInputs(Table table) {
        throw new UnsupportedOperationException();
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
    public List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
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
    public List<String> getColumns() {
        return Collections.singletonList(sourceName);
    }

    @Override
    public List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public ColumnSource<D> getDataView() {
        return new ViewColumnSource<>(destDataType, componentType, new Formula(null) {
            @Override
            public Object getPrev(long rowKey) {
                return function.apply(rowKey, sourceColumnSource.getPrev(rowKey));
            }

            @Override
            public Object get(long rowKey) {
                return function.apply(rowKey, sourceColumnSource.get(rowKey));
            }

            @Override
            public ChunkType getChunkType() {
                return ChunkType.fromElementType(destDataType);
            }

            @Override
            public FillContext makeFillContext(int chunkCapacity) {
                // Not sure this is right.
                return new FunctionalColumnFillContext(getChunkType());
            }

            @Override
            public void fillChunk(@NotNull FillContext fillContext,
                    @NotNull final WritableChunk<? super Values> destination,
                    @NotNull final RowSequence rowSequence) {
                final FunctionalColumnFillContext ctx = (FunctionalColumnFillContext) fillContext;
                ctx.chunkFiller.fillByIndices(this, rowSequence, destination);
            }

            @Override
            public void fillPrevChunk(@NotNull FillContext fillContext,
                    @NotNull final WritableChunk<? super Values> destination,
                    @NotNull final RowSequence rowSequence) {
                final FunctionalColumnFillContext ctx = (FunctionalColumnFillContext) fillContext;
                ctx.chunkFiller.fillByIndices(this, rowSequence, destination);
            }
        }, false);
    }

    private static class FunctionalColumnFillContext implements Formula.FillContext {
        final ChunkFiller chunkFiller;

        FunctionalColumnFillContext(final ChunkType chunkType) {
            chunkFiller = ChunkFiller.forChunkType(chunkType);
        }
    }

    @NotNull
    @Override
    public ColumnSource<?> getLazyView() {
        // TODO: memoize
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
    public WritableColumnSource<?> newDestInstance(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WritableColumnSource<?> newFlatDestInstance(long size) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRetain() {
        return false;
    }

    @Override
    public boolean disallowRefresh() {
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
