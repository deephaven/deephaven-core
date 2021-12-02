package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.ColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.RowIdSource;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import io.deephaven.chunk.attributes.Values;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * {@link RegionedColumnSourceObject} with support for dictionary access via {@link SymbolTableSource} methods. Note
 * that it may not be the case that all values are stored as dictionary offsets. See {@link #hasSymbolTable(RowSet)}.
 */
class RegionedColumnSourceWithDictionary<DATA_TYPE>
        extends RegionedColumnSourceObject.AsValues<DATA_TYPE>
        implements SymbolTableSource<DATA_TYPE> {

    RegionedColumnSourceWithDictionary(@NotNull final Class<DATA_TYPE> dataType,
            @Nullable final Class<?> componentType) {
        super(dataType, componentType);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || super.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        // noinspection unchecked
        return alternateDataType == long.class ? (ColumnSource<ALTERNATE_DATA_TYPE>) new AsLong()
                : super.doReinterpret(alternateDataType);
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
    }

    private final class AsLong
            extends RegionedColumnSourceBase<Long, DictionaryKeys, ColumnRegionLong<DictionaryKeys>>
            implements ColumnSourceGetDefaults.ForLong {

        private final ColumnRegionLong<DictionaryKeys> nullRegion;
        private volatile ColumnRegionLong<DictionaryKeys>[] wrapperRegions;

        private AsLong() {
            super(long.class);
            nullRegion = ColumnRegionLong.createNull(PARAMETERS.regionMask);
            // noinspection unchecked
            wrapperRegions = new ColumnRegionLong[0];
        }

        @Override
        public long getLong(final long elementIndex) {
            return (elementIndex == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(elementIndex))
                    .getLong(elementIndex);
        }

        @Override
        public int addRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation) {
            return RegionedColumnSourceWithDictionary.this.addRegion(columnDefinition, columnLocation);
        }

        @Override
        <OTHER_REGION_TYPE> int addRegionForUnitTests(@NotNull final OTHER_REGION_TYPE region) {
            return RegionedColumnSourceWithDictionary.this.addRegionForUnitTests(region);
        }

        @NotNull
        @Override
        ColumnRegionLong<DictionaryKeys> getNullRegion() {
            return nullRegion;
        }

        @Override
        public int getRegionCount() {
            return RegionedColumnSourceWithDictionary.this.getRegionCount();
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getRegion(final int regionIndex) {
            final ColumnRegionObject<DATA_TYPE, Values> sourceRegion =
                    RegionedColumnSourceWithDictionary.this.getRegion(regionIndex);
            if (sourceRegion instanceof ColumnRegion.Null) {
                return nullRegion;
            }
            ColumnRegionLong<DictionaryKeys>[] localWrappers;
            ColumnRegionLong<DictionaryKeys> wrapper;
            if ((localWrappers = wrapperRegions).length > regionIndex
                    && (wrapper = localWrappers[regionIndex]) != null) {
                return wrapper;
            }
            synchronized (this) {
                if ((localWrappers = wrapperRegions).length > regionIndex
                        && (wrapper = localWrappers[regionIndex]) != null) {
                    return wrapper;
                }
                if (localWrappers.length <= regionIndex) {
                    wrapperRegions = localWrappers =
                            Arrays.copyOf(localWrappers, Math.min(regionIndex + 1 << 1, getRegionCount()));
                }
                return localWrappers[regionIndex] =
                        ColumnRegionObject.DictionaryKeysWrapper.create(parameters(), regionIndex, sourceRegion);
            }
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == RegionedColumnSourceWithDictionary.this.getType();
        }

        @Override
        protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                @NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) RegionedColumnSourceWithDictionary.this;
        }

        @Override
        @OverridingMethodsMustInvokeSuper
        public void releaseCachedResources() {
            super.releaseCachedResources();
            // We are a reinterpreted column of RegionedColumnSourceObjectReferencing.this, so if we're asked to release
            // our resources, release the real resources in the underlying column.
            RegionedColumnSourceWithDictionary.this.releaseCachedResources();
            final ColumnRegionLong<DictionaryKeys>[] localWrappers = wrapperRegions;
            // noinspection unchecked
            wrapperRegions = new ColumnRegionLong[0];
            Arrays.stream(localWrappers).filter(Objects::nonNull).forEach(Releasable::releaseCachedResources);
        }
    }

    private final class AsDictionary
            extends RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionObject<DATA_TYPE, Values>>
            implements ColumnSourceGetDefaults.ForObject<DATA_TYPE> {

        private AsDictionary() {
            super(RegionedColumnSourceWithDictionary.this.getType(),
                    RegionedColumnSourceWithDictionary.this.getComponentType());
        }

        @Override
        public DATA_TYPE get(final long elementIndex) {
            return (elementIndex == RowSequence.NULL_ROW_KEY ? getNullRegion() : lookupRegion(elementIndex))
                    .getObject(elementIndex);
        }

        @Override
        public int addRegion(@NotNull final ColumnDefinition<?> columnDefinition,
                @NotNull final ColumnLocation columnLocation) {
            return RegionedColumnSourceWithDictionary.this.addRegion(columnDefinition, columnLocation);
        }

        @Override
        <OTHER_REGION_TYPE> int addRegionForUnitTests(@NotNull final OTHER_REGION_TYPE region) {
            return RegionedColumnSourceWithDictionary.this.addRegionForUnitTests(region);
        }

        @NotNull
        @Override
        ColumnRegionObject<DATA_TYPE, Values> getNullRegion() {
            return RegionedColumnSourceWithDictionary.this.getNullRegion();
        }

        @Override
        public int getRegionCount() {
            return RegionedColumnSourceWithDictionary.this.getRegionCount();
        }

        @Override
        public ColumnRegionObject<DATA_TYPE, Values> getRegion(final int regionIndex) {
            // ColumnRegionObject implementations are expected to cache the result of getDictionaryValuesRegion(),
            // so it's fine to call more than once and avoid extra backing storage in the column source.
            return RegionedColumnSourceWithDictionary.this.getRegion(regionIndex).getDictionaryValuesRegion();
        }
    }

    @Override
    public boolean hasSymbolTable(@NotNull final RowSet sourceIndex) {
        if (sourceIndex.isEmpty()) {
            // Trivially true
            return true;
        }
        RegionVisitResult result;
        try (final RowSet.SearchIterator keysToVisit = sourceIndex.searchIterator()) {
            keysToVisit.nextLong(); // Safe, since sourceIndex must be non-empty
            do {
                result = lookupRegion(keysToVisit.currentValue()).supportsDictionaryFormat(keysToVisit);
            } while (result == RegionVisitResult.CONTINUE);
        }
        return result != RegionVisitResult.FAILED;
    }

    @Override
    public QueryTable getStaticSymbolTable(@NotNull RowSet sourceIndex, boolean useLookupCaching) {
        // NB: We assume that hasSymbolTable has been tested by the caller
        final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionObject<DATA_TYPE, Values>> dictionaryColumn =
                new AsDictionary();

        final TrackingRowSet symbolTableRowSet;
        if (sourceIndex.isEmpty()) {
            symbolTableRowSet = RowSetFactory.empty().toTracking();
        } else {
            final RowSetBuilderSequential symbolTableIndexBuilder = RowSetFactory.builderSequential();
            try (final RowSet.SearchIterator keysToVisit = sourceIndex.searchIterator()) {
                keysToVisit.nextLong(); // Safe, since sourceIndex must be non-empty
                do {
                    dictionaryColumn.lookupRegion(keysToVisit.currentValue()).gatherDictionaryValuesRowSet(keysToVisit,
                            RowSequenceFactory.EMPTY_ITERATOR, symbolTableIndexBuilder);
                } while (keysToVisit.hasNext());
            }
            symbolTableRowSet = symbolTableIndexBuilder.build().toTracking();
        }

        final Map<String, ColumnSource<?>> symbolTableColumnSources = new LinkedHashMap<>();
        symbolTableColumnSources.put(SymbolTableSource.ID_COLUMN_NAME, new RowIdSource());
        symbolTableColumnSources.put(SymbolTableSource.SYMBOL_COLUMN_NAME, dictionaryColumn);

        return new QueryTable(symbolTableRowSet, symbolTableColumnSources);
    }

    @Override
    public final Table getSymbolTable(@NotNull final QueryTable sourceTable, final boolean useLookupCaching) {
        // NB: We assume that hasSymbolTable has been tested by the caller, and that for refreshing tables it will
        // remain true.
        return sourceTable.memoizeResult(MemoizedOperationKey.symbolTable(this, useLookupCaching), () -> {
            final String description = "getSymbolTable(" + sourceTable.getDescription() + ", " + useLookupCaching + ')';
            return QueryPerformanceRecorder.withNugget(description, sourceTable.size(), () -> {
                final SwapListener swapListener =
                        sourceTable.createSwapListenerIfRefreshing(SwapListener::new);
                final Mutable<Table> result = new MutableObject<>();
                sourceTable.initializeWithSnapshot(description, swapListener,
                        (final boolean usePrev, final long beforeClockValue) -> {
                            final QueryTable symbolTable;
                            if (swapListener == null) {
                                symbolTable = getStaticSymbolTable(sourceTable.getRowSet(), useLookupCaching);
                            } else {
                                symbolTable = getStaticSymbolTable(
                                        usePrev ? sourceTable.getRowSet().copyPrev() : sourceTable.getRowSet(),
                                        useLookupCaching);
                                swapListener.setListenerAndResult(
                                        new SymbolTableUpdateListener(description, sourceTable, symbolTable),
                                        symbolTable);
                                symbolTable.addParentReference(swapListener);
                            }
                            result.setValue(symbolTable);
                            return true;
                        });
                return result.getValue();
            });
        });
    }

    private final class SymbolTableUpdateListener extends BaseTable.ListenerImpl {

        private final BaseTable symbolTable;
        private final ModifiedColumnSet emptyModifiedColumns;

        private SymbolTableUpdateListener(@NotNull final String description, @NotNull final Table sourceTable,
                @NotNull final QueryTable symbolTable) {
            super(description, sourceTable, symbolTable);
            this.symbolTable = symbolTable;
            this.emptyModifiedColumns = symbolTable.newModifiedColumnSet();
        }

        @Override
        public void onUpdate(@NotNull final TableUpdate upstream) {
            // TODO: Consider updating this to use:
            // io.deephaven.engine.table.impl.verify.TableAssertions.assertAppendOnly(java.lang.String,
            // io.deephaven.engine.table.Table)
            if (upstream.removed().isNonempty() || upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                throw new IllegalStateException("Source table for a regioned symbol table should be add-only, instead "
                        + "removed=" + upstream.removed() + ", modified=" + upstream.modified() + ", shifted="
                        + upstream.shifted());
            }
            if (upstream.added().isEmpty()) {
                return;
            }

            final RowSetBuilderSequential symbolTableAddedBuilder = RowSetFactory.builderSequential();
            final RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionObject<DATA_TYPE, Values>> dictionaryColumn =
                    (RegionedColumnSourceBase<DATA_TYPE, Values, ColumnRegionObject<DATA_TYPE, Values>>) symbolTable
                            .getColumnSource(SymbolTableSource.SYMBOL_COLUMN_NAME);

            try (final RowSet.SearchIterator keysToVisit = upstream.added().searchIterator();
                    final RowSequence.Iterator knownKeys = symbolTable.getRowSet().getRowSequenceIterator()) {
                keysToVisit.nextLong(); // Safe, since sourceIndex must be non-empty
                do {
                    dictionaryColumn.lookupRegion(keysToVisit.currentValue()).gatherDictionaryValuesRowSet(keysToVisit,
                            knownKeys, symbolTableAddedBuilder);
                } while (keysToVisit.hasNext());
            }

            final RowSet symbolTableAdded = symbolTableAddedBuilder.build();
            if (symbolTableAdded.isNonempty()) {
                symbolTable.getRowSet().writableCast().insert(symbolTableAdded);
                symbolTable.notifyListeners(new TableUpdateImpl(symbolTableAdded, RowSetFactory.empty(),
                        RowSetFactory.empty(), RowSetShiftData.EMPTY, emptyModifiedColumns));
            } else {
                symbolTableAdded.close();
            }
        }
    }
}
