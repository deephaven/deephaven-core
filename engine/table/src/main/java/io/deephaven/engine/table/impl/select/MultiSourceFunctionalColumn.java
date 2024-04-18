//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.*;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.PrevColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;

public class MultiSourceFunctionalColumn<D> implements SelectColumn {

    private final List<String> sourceNames;
    private ColumnSource<?>[] sourceColumns;
    private ColumnSource<?>[] prevSources;

    @NotNull
    private final String destName;
    @NotNull
    private final Class<D> destDataType;
    @NotNull
    private final RowKeyAndSourcesFunction<D> function;
    @Nullable
    private final Class<?> componentType;

    @FunctionalInterface
    public interface RowKeyAndSourcesFunction<D> {
        D apply(long rowKey, ColumnSource<?>[] sources);
    }

    public MultiSourceFunctionalColumn(
            @NotNull List<String> sourceNames,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull RowKeyAndSourcesFunction<D> function) {
        this(sourceNames, destName, destDataType, null, function);
    }

    public MultiSourceFunctionalColumn(
            @NotNull List<String> sourceNames,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @Nullable Class<?> componentType,
            @NotNull RowKeyAndSourcesFunction<D> function) {
        this.sourceNames = sourceNames.stream()
                .map(NameValidator::validateColumnName)
                .collect(Collectors.toList());

        this.destName = NameValidator.validateColumnName(destName);
        this.destDataType = Require.neqNull(destDataType, "destDataType");
        this.componentType = componentType;
        this.function = function;
        Require.gtZero(destName.length(), "destName.length()");
    }

    @Override
    public String toString() {
        return "function(" + String.join(",", sourceNames) + ',' + destName + ')';
    }

    @Override
    public List<String> initInputs(TrackingRowSet rowSet, Map<String, ? extends ColumnSource<?>> columnsOfInterest) {
        final List<ColumnSource<?>> localSources = new ArrayList<>(sourceNames.size());
        final List<ColumnSource<?>> localPrev = new ArrayList<>(sourceNames.size());

        // the column overrides occur when we are in the midst of an update; but we only reinterpret columns with an
        // updateView, not as part of a generalized update. Thus if this is happening our assumptions have been
        // violated and we could provide the wrong answer by not paying attention to the columnsOverride
        sourceNames.forEach(name -> {
            final ColumnSource<?> localSourceColumnSource = columnsOfInterest.get(name);
            if (localSourceColumnSource == null) {
                throw new IllegalArgumentException("Source column " + name + " doesn't exist!");
            }

            localSources.add(localSourceColumnSource);
            localPrev.add(new PrevColumnSource<>(localSourceColumnSource));
        });

        sourceColumns = localSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        prevSources = localPrev.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);

        return getColumns();
    }

    @Override
    public List<String> initDef(@NotNull final Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        NoSuchColumnException.throwIf(columnDefinitionMap.keySet(), sourceNames);
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
        return Collections.unmodifiableList(sourceNames);
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
            public Object get(long rowKey) {
                return function.apply(rowKey, sourceColumns);
            }

            @Override
            public Object getPrev(long rowKey) {
                return function.apply(rowKey, prevSources);
            }

            @Override
            public ChunkType getChunkType() {
                return ChunkType.fromElementType(destDataType);
            }

            @Override
            public FillContext makeFillContext(int chunkCapacity) {
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
    public MultiSourceFunctionalColumn<D> copy() {
        return new MultiSourceFunctionalColumn<>(sourceNames, destName, destDataType, function);
    }
}
