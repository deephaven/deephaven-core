package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ColumnSourceGetDefaults;
import io.deephaven.db.v2.sources.RowIdSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.DictionaryKeys;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;

/**
 * {@link RegionedColumnSourceObject} with support for dictionary access via {@link SymbolTableSource} methods.
 * Note that it may not be the case that all values are stored as dictionary offsets.
 * See {@link #hasSymbolTable(ReadOnlyIndex)}.
 */
class RegionedColumnSourceWithDictionary<DATA_TYPE>
        extends RegionedColumnSourceObject.AsValues<DATA_TYPE>
        implements SymbolTableSource<DATA_TYPE> {

    RegionedColumnSourceWithDictionary(@NotNull final Class<DATA_TYPE> dataType, @Nullable final Class<?> componentType) {
        super(dataType, componentType);
    }
//
//    @TestUseOnly
//    RegionedColumnSourceWithDictionary(@NotNull Class<DATA_TYPE> dataType,
//                                             RegionedColumnSourceBase<DATA_TYPE, Attributes.Values, ColumnRegionObject<DATA_TYPE, Attributes.Values>> dictionaryColumn) {
//        super(ColumnRegionInt.createNull(PARAMETERS.regionMask), dataType, RegionedColumnSourceDictionaryKey::new);
//        this.dictionaryColumn = dictionaryColumn;
//    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || super.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        //noinspection unchecked
        return alternateDataType == long.class ? (ColumnSource<ALTERNATE_DATA_TYPE>) new AsLong() : super.doReinterpret(alternateDataType);
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
    }

    final class AsLong extends RegionedColumnSourceBase<Long, DictionaryKeys, ColumnRegionLong<DictionaryKeys>>
            implements ColumnSourceGetDefaults.ForLong {

        AsLong() {
            super(long.class);
        }

        @Override
        public long getLong(final long elementIndex) {
            return (elementIndex == NULL_KEY ? getNullRegion() : lookupRegion(elementIndex)).getLong(elementIndex);
        }

        @Override
        public int addRegion(@NotNull final ColumnDefinition<?> columnDefinition, @NotNull final ColumnLocation columnLocation) {
            return RegionedColumnSourceWithDictionary.this.addRegion(columnDefinition, columnLocation);
        }

        @Override
        <OTHER_REGION_TYPE> int addRegionForUnitTests(@NotNull final OTHER_REGION_TYPE region) {
            return RegionedColumnSourceWithDictionary.this.addRegionForUnitTests(region);
        }

        @NotNull
        @Override
        ColumnRegionLong<DictionaryKeys> getNullRegion() {
            return ColumnRegionLong.createNull(parameters().regionMask);
        }

        @Override
        public int getRegionCount() {
            return RegionedColumnSourceWithDictionary.this.getRegionCount();
        }

        @Override
        public ColumnRegionLong<DictionaryKeys> getRegion(final int regionIndex) {
            return ColumnRegionObject.DictionaryKeysWrapper.create(parameters(), regionIndex, RegionedColumnSourceWithDictionary.this.getRegion(regionIndex));
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == RegionedColumnSourceWithDictionary.this.getType();
        }

        @Override
        protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            //noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) RegionedColumnSourceWithDictionary.this;
        }

        @Override
        @OverridingMethodsMustInvokeSuper
        public void releaseCachedResources() {
            // We are a reinterpreted column of RegionedColumnSourceObjectReferencing.this, so if we're asked to release
            // our resources, release the real resources in the underlying column.
            super.releaseCachedResources();
            RegionedColumnSourceWithDictionary.this.releaseCachedResources();
        }
    }

    @Override
    public boolean hasSymbolTable(@NotNull final ReadOnlyIndex sourceIndex) {
        try (final OrderedKeys.Iterator sourceIterator = sourceIndex.getOrderedKeysIterator()) {
            while (sourceIterator.hasMore()) {
                if (lookupRegion(sourceIterator.peekNextKey()).supportsDictionaryFormat(sourceIterator, true)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public QueryTable getStaticSymbolTable(@NotNull ReadOnlyIndex sourceIndex, boolean useLookupCaching) {
        final Index.SequentialBuilder symbolTableIndexBuilder = Index.FACTORY.getSequentialBuilder();

        try (Index.SearchIterator sourceIndexIterator = sourceIndex.searchIterator()) {
            for (int regionIndex = 0; sourceIndexIterator.advance(RegionedColumnSource.getFirstElementIndex(regionIndex)); ++regionIndex) {
                long sourceElementIndex = sourceIndexIterator.currentValue();
                regionIndex = getRegionIndex(sourceElementIndex);
                ColumnRegionObject<DATA_TYPE, Attributes.Values> region = dictionaryColumn.getRegion(regionIndex);

                if (region.length() > 0) {
                    symbolTableIndexBuilder.appendRange(region.firstRow(sourceElementIndex), region.lastRow(sourceElementIndex));
                }
            }
        }

        final Map<String, ColumnSource<?>> symbolTableColumnSources = new LinkedHashMap<>();
        symbolTableColumnSources.put(SymbolTableSource.ID_COLUMN_NAME, new RowIdSource());
        symbolTableColumnSources.put(SymbolTableSource.SYMBOL_COLUMN_NAME, useLookupCaching ? dictionaryColumn :
                new RegionedColumnSourceSkipCache<>(type, dictionaryColumn));

        return new QueryTable(symbolTableIndexBuilder.getIndex(), symbolTableColumnSources);
    }

    @Override
    public final Table getSymbolTable(@NotNull final QueryTable sourceTable, final boolean useLookupCaching) {
        return sourceTable.memoizeResult(MemoizedOperationKey.symbolTable(this, useLookupCaching), () -> {
            final String description = "getSymbolTable(" + sourceTable.getDescription() + ", " + useLookupCaching + ')';
            return QueryPerformanceRecorder.withNugget(description, sourceTable.size(), () -> {
                final ShiftAwareSwapListener swapListener = sourceTable.createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);
                final Mutable<Table> result = new MutableObject<>();
                sourceTable.initializeWithSnapshot(description, swapListener, (final boolean usePrev, final long beforeClockValue) -> {
                    final QueryTable symbolTable;
                    if (swapListener == null) {
                        symbolTable = getStaticSymbolTable(sourceTable.getIndex(), useLookupCaching);
                    } else {
                        symbolTable = getStaticSymbolTable(usePrev ? sourceTable.getIndex().getPrevIndex() : sourceTable.getIndex(), useLookupCaching);
                        swapListener.setListenerAndResult(new SymbolTableUpdateListener(description, sourceTable, symbolTable), symbolTable);
                        symbolTable.addParentReference(swapListener);
                    }
                    result.setValue(symbolTable);
                    return true;
                });
                return result.getValue();
            });
        });
    }

    private final class SymbolTableUpdateListener extends BaseTable.ShiftAwareListenerImpl {

        private final BaseTable symbolTable;
        private final ModifiedColumnSet emptyModifiedColumns;

        private SymbolTableUpdateListener(@NotNull final String description, @NotNull final DynamicTable sourceTable, @NotNull final BaseTable symbolTable) {
            super(description, sourceTable, symbolTable);
            this.symbolTable = symbolTable;
            this.emptyModifiedColumns = symbolTable.newModifiedColumnSet();
        }

        @Override
        public void onUpdate(@NotNull final Update upstream) {
            // TODO-RWC: Update and use io.deephaven.db.tables.verify.TableAssertions.assertAppendOnly(java.lang.String, io.deephaven.db.tables.Table) ?
            if (upstream.removed.nonempty() || upstream.modified.nonempty() || upstream.shifted.nonempty()) {
                throw new IllegalStateException("Source table for a regioned symbol table should be add-only, instead "
                        + "removed=" + upstream.removed + ", modified=" + upstream.modified + ", shifted=" + upstream.shifted);
            }
            if (upstream.added.empty()) {
                return;
            }
            final Index.SequentialBuilder symbolTableAddedBuilder = Index.FACTORY.getSequentialBuilder();

            try (Index.SearchIterator sourceAddedIterator = upstream.added.searchIterator();
                 OrderedKeys.Iterator symbolTableOKI = symbolTable.getIndex().getOrderedKeysIterator()) {
                sourceAddedIterator.nextLong();
                for (int regionIndex = 0; sourceAddedIterator.advance(RegionedColumnSource.getFirstElementIndex(regionIndex)); ++regionIndex) {
                    final long sourceElementIndex = sourceAddedIterator.currentValue();
                    regionIndex = getRegionIndex(sourceElementIndex);
                    ColumnRegionObject<DATA_TYPE, Attributes.Values> region = dictionaryColumn.getRegion(regionIndex);

                    if (region.length() > 0) {
                        final long regionFirstSymbolIndex = region.firstRow(sourceElementIndex);
                        final long regionLastSymbolIndex = region.lastRow(sourceElementIndex);
                        final OrderedKeys precedingOrderedKeys = symbolTableOKI.getNextOrderedKeysThrough(regionLastSymbolIndex);
                        final long precedingSymbolIndex = precedingOrderedKeys.lastKey();
                        final long regionFirstAddedSymbolIndex = precedingSymbolIndex == NULL_KEY || precedingSymbolIndex < regionFirstSymbolIndex ? regionFirstSymbolIndex : precedingSymbolIndex + 1;
                        if (regionFirstAddedSymbolIndex <= regionLastSymbolIndex) {
                            symbolTableAddedBuilder.appendRange(regionFirstAddedSymbolIndex, regionLastSymbolIndex);
                        }
                    }
                }
            }

            final Index symbolTableAdded = symbolTableAddedBuilder.getIndex();
            if (symbolTableAdded.nonempty()) {
                symbolTable.getIndex().insert(symbolTableAdded);
                symbolTable.notifyListeners(new Update(symbolTableAdded, Index.FACTORY.getEmptyIndex(),
                        Index.FACTORY.getEmptyIndex(), IndexShiftData.EMPTY, emptyModifiedColumns));
            }
        }
    }
}
