//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.api.util.NameValidator;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.function.ToLongFunction;

public class FunctionalColumnLong<S> implements SelectColumn {

    @NotNull
    private final String sourceName;
    @NotNull
    private final Class<S> sourceDataType;
    @NotNull
    private final String destName;
    @NotNull
    private final RowKeyAndValueFunction<S> function;

    private ColumnSource<S> sourceColumnSource;

    @FunctionalInterface
    public interface RowKeyAndValueFunction<S> {
        long applyAsLong(long rowKey, S value);
    }

    public FunctionalColumnLong(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull ToLongFunction<S> function) {
        this(sourceName, sourceDataType, destName, (l, v) -> function.applyAsLong(v));
    }

    public FunctionalColumnLong(
            @NotNull String sourceName,
            @NotNull Class<S> sourceDataType,
            @NotNull String destName,
            @NotNull RowKeyAndValueFunction<S> function) {
        this.sourceName = NameValidator.validateColumnName(sourceName);
        this.sourceDataType = Require.neqNull(sourceDataType, "sourceDataType");
        this.destName = NameValidator.validateColumnName(destName);
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
                .isAssignableFrom(TypeUtils.getBoxedType(localSourceColumnSource.getType())))) {
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
        return long.class;
    }

    @Override
    public Class<?> getReturnedComponentType() {
        return null;
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
    public ColumnSource<Long> getDataView() {
        return new ViewColumnSource<>(long.class, new Formula(null) {

            @Override
            public long getLong(final long rowKey) {
                return function.applyAsLong(rowKey, sourceColumnSource.get(rowKey));
            }

            @Override
            public long getPrevLong(final long rowKey) {
                return function.applyAsLong(rowKey, sourceColumnSource.getPrev(rowKey));
            }

            @Override
            public Object get(final long rowKey) {
                return TypeUtils.box(getLong(rowKey));
            }

            @Override
            public Object getPrev(final long rowKey) {
                return TypeUtils.box(getPrevLong(rowKey));
            }

            @Override
            public ChunkType getChunkType() {
                return ChunkType.Long;
            }

            @Override
            public FillContext makeFillContext(final int chunkCapacity) {
                return new FunctionalColumnFillContext();
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

        private FunctionalColumnFillContext() {
            chunkFiller = ChunkFiller.forChunkType(ChunkType.Long);
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
        return SparseArrayColumnSource.getSparseMemoryColumnSource(size, long.class);
    }

    @Override
    public final WritableColumnSource<?> newFlatDestInstance(final long size) {
        return InMemoryColumnSource.getImmutableMemoryColumnSource(size, long.class, null);
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
    public FunctionalColumnLong<S> copy() {
        return new FunctionalColumnLong<>(sourceName, sourceDataType, destName, function);
    }
}
