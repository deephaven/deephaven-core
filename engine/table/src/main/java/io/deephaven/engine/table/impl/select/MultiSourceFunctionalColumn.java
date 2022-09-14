/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.*;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.PrevColumnSource;
import io.deephaven.engine.table.impl.sources.ViewColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

// TODO: Comment the heck out of this...
public class MultiSourceFunctionalColumn<D> implements SelectColumn {
    private final List<String> sourceNames;
    private ColumnSource<?>[] sourceColumns;
    private ColumnSource<?>[] prevSources;

    @NotNull
    private final String destName;
    @NotNull
    private final Class<D> destDataType;
    @NotNull
    private final BiFunction<Long, ColumnSource<?>[], D> function;
    @NotNull
    private final Class<?> componentType;

    boolean usesPython;

    public MultiSourceFunctionalColumn(@NotNull List<String> sourceNames,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull BiFunction<Long, ColumnSource<?>[], D> function) {
        this(sourceNames, destName, destDataType, Object.class, function);
    }

    public MultiSourceFunctionalColumn(@NotNull List<String> sourceNames,
            @NotNull String destName,
            @NotNull Class<D> destDataType,
            @NotNull Class<?> componentType,
            @NotNull BiFunction<Long, ColumnSource<?>[], D> function) {
        this.sourceNames = sourceNames.stream()
                .map(NameValidator::validateColumnName)
                .collect(Collectors.toList());

        this.destName = NameValidator.validateColumnName(destName);
        this.destDataType = Require.neqNull(destDataType, "destDataType");
        this.componentType = Require.neqNull(componentType, "componentType");
        this.function = function;
        Require.gtZero(destName.length(), "destName.length()");
    }

    @Override
    public String toString() {
        return "function(" + String.join(",", sourceNames) + ',' + destName + ')';
    }

    @Override
    public List<String> initInputs(Table table) {
        throw new UnsupportedOperationException();
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
    public List<String> initDef(Map<String, ColumnDefinition<?>> columnDefinitionMap) {
        final MutableObject<List<String>> missingColumnsHolder = new MutableObject<>();
        sourceNames.forEach(name -> {
            final ColumnDefinition<?> sourceColumnDefinition = columnDefinitionMap.get(name);
            if (sourceColumnDefinition == null) {
                List<String> missingColumnsList;
                if ((missingColumnsList = missingColumnsHolder.getValue()) == null) {
                    missingColumnsHolder.setValue(missingColumnsList = new ArrayList<>());
                }
                missingColumnsList.add(name);
            }
        });

        if (missingColumnsHolder.getValue() != null) {
            throw new NoSuchColumnException(columnDefinitionMap.keySet(), missingColumnsHolder.getValue());
        }

        return getColumns();
    }

    @Override
    public Class<?> getReturnedType() {
        return destDataType;
    }

    @Override
    public List<String> getColumns() {
        return Collections.unmodifiableList(sourceNames);
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
                return function.apply(rowKey, prevSources);
            }

            @Override
            public Object get(long rowKey) {
                return function.apply(rowKey, sourceColumns);
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
    public MultiSourceFunctionalColumn<D> copy() {
        return new MultiSourceFunctionalColumn<>(sourceNames, destName, destDataType, function);
    }
}
