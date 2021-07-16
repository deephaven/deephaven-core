/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.select;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.utils.NameValidator;
import io.deephaven.db.v2.NoSuchColumnException;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.PrevColumnSource;
import io.deephaven.db.v2.sources.ViewColumnSource;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

// TODO: Comment the heck out of this...
public class MultiSourceFunctionalColumn<D> implements SelectColumn {
    private final List<String> sourceNames;
    private ColumnSource<?>[] sourceColumns;
    private ColumnSource<?>[] prevSources;

    @NotNull private final String destName;
    @NotNull private final Class<D> destDataType;
    @NotNull private final BiFunction<Long, ColumnSource[], D> function;
    @NotNull private final Class componentType;

    public MultiSourceFunctionalColumn(@NotNull List<String> sourceNames,
                                       @NotNull String destName,
                                       @NotNull Class<D> destDataType,
                                       @NotNull BiFunction<Long, ColumnSource[], D> function) {
        this(sourceNames, destName, destDataType, Object.class, function);
    }

    public MultiSourceFunctionalColumn(@NotNull List<String> sourceNames,
                                       @NotNull String destName,
                                       @NotNull Class<D> destDataType,
                                       @NotNull Class componentType,
                                       @NotNull BiFunction<Long, ColumnSource[], D> function) {
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
    public List<String> initInputs(Index index, Map<String, ? extends ColumnSource> columnsOfInterest) {
        if(sourceColumns == null) {
            final List<ColumnSource<?>> localSources = new ArrayList<>(sourceNames.size());
            final List<ColumnSource<?>> localPrev = new ArrayList<>(sourceNames.size());

            // the column overrides occur when we are in the midst of an update; but we only reinterpret columns with an
            // updateView, not as part of a generalized update.  Thus if this is happening our assumptions have been violated
            // and we could provide the wrong answer by not paying attention to the columnsOverride
            sourceNames.forEach(name -> {
                final ColumnSource localSourceColumnSource = columnsOfInterest.get(name);
                if (localSourceColumnSource == null) {
                    throw new IllegalArgumentException("Source column " + name + " doesn't exist!");
                }

                localSources.add(localSourceColumnSource);
                //noinspection unchecked
                localPrev.add(new PrevColumnSource<>(localSourceColumnSource));
            });

            sourceColumns = localSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
            prevSources = localPrev.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        }

        return getColumns();
    }

    @Override
    public List<String> initDef(Map<String, ColumnDefinition> columnDefinitionMap) {
        final MutableObject<List<String>> missingColumnsHolder = new MutableObject<>();
        sourceNames.forEach(name -> {
            final ColumnDefinition sourceColumnDefinition = columnDefinitionMap.get(name);
            if(sourceColumnDefinition == null) {
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
    public Class getReturnedType() {
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
    public ColumnSource getDataView() {
        return new ViewColumnSource<>(destDataType, componentType, new Formula(null) {
            @Override
            public Object getPrev(long key) {
                return function.apply(key, prevSources);
            }

            @Override
            public Object get(long key) {
                return function.apply(key, sourceColumns);
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
                                  @NotNull final OrderedKeys orderedKeys) {
                final FunctionalColumnFillContext ctx = (FunctionalColumnFillContext)fillContext;
                ctx.chunkFiller.fillByIndices(this, orderedKeys, destination);
            }

            @Override
            public void fillPrevChunk(@NotNull FillContext fillContext,
                                      @NotNull final WritableChunk<? super Values> destination,
                                      @NotNull final OrderedKeys orderedKeys) {
                final FunctionalColumnFillContext ctx = (FunctionalColumnFillContext)fillContext;
                ctx.chunkFiller.fillByIndices(this, orderedKeys, destination);
            }
        });
    }

    private static class FunctionalColumnFillContext implements Formula.FillContext {
        final ChunkFiller chunkFiller;

        FunctionalColumnFillContext(final ChunkType chunkType) {
            chunkFiller = ChunkFiller.fromChunkType(chunkType);
        }
    }

    @NotNull
    @Override
    public ColumnSource getLazyView() {
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
    public WritableSource newDestInstance(long size) {
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
    public MultiSourceFunctionalColumn<D> copy() {
        return new MultiSourceFunctionalColumn<>(sourceNames, destName, destDataType, function);
    }
}
