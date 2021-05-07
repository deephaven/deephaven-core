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
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.codec.ObjectDecoder;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.deephaven.db.v2.utils.ReadOnlyIndex.NULL_KEY;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;

class RegionedColumnSourceObjectWithDictionary<T>
        extends RegionedColumnSourceReferencing<T, Attributes.DictionaryKeys, Integer, ColumnRegionInt<Attributes.DictionaryKeys>>
        implements ColumnSourceGetDefaults.ForObject<T>, SymbolTableSource<T> {

    private final RegionedColumnSourceBase<T, Attributes.Values, ColumnRegionObject<T, Attributes.Values>> dictionaryColumn;

    RegionedColumnSourceObjectWithDictionary(@NotNull Class<T> dataType, ObjectDecoder<T> decoder) {
        super(ColumnRegionInt.createNull(), dataType, RegionedColumnSourceDictionaryKey::new);
        dictionaryColumn = RegionedColumnSourceSymbol.createWithLookupCache(decoder, dataType, true);
    }

    @TestUseOnly
    RegionedColumnSourceObjectWithDictionary(@NotNull Class<T> dataType,
                                             RegionedColumnSourceBase<T, Attributes.Values, ColumnRegionObject<T, Attributes.Values>> dictionaryColumn) {
        super(ColumnRegionInt.createNull(), dataType, RegionedColumnSourceDictionaryKey::new);
        this.dictionaryColumn = dictionaryColumn;
    }

    @Override
    public int addRegion(@NotNull ColumnDefinition<?> columnDefinition, @NotNull ColumnLocation<?> columnLocation) {
        dictionaryColumn.addRegion(columnDefinition, columnLocation);
        return super.addRegion(columnDefinition, columnLocation);
    }

    @Override
    public void convertRegion(WritableChunk<? super Attributes.DictionaryKeys> destination,
                              Chunk<? extends Attributes.DictionaryKeys> source,
                              OrderedKeys orderedKeys) {

        WritableObjectChunk<T, ? super Attributes.DictionaryKeys> objectChunk = destination.asWritableObjectChunk();
        IntChunk<? extends Attributes.Values> intSource = source.asIntChunk();

        ColumnRegionObject<T, Attributes.Values> region = dictionaryColumn.lookupRegion(orderedKeys.firstKey());

        final int size = objectChunk.size();
        final int length = intSource.size();

        for (int i = 0; i < length; ++i) {
            int key = intSource.get(i);
            objectChunk.set(size + i, key == NULL_INT ? null : region.getObject(key));
        }
        objectChunk.setSize(size + length);
    }

    @Override
    public T get(long elementIndex) {
        int key = elementIndex == NULL_KEY ? NULL_INT : lookupRegion(elementIndex).getReferencedRegion().getInt(elementIndex);
        return key == NULL_INT ? null : dictionaryColumn.lookupRegion(elementIndex).getObject(key);
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || super.allowsReinterpret(alternateDataType);
    }

    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        //noinspection unchecked
        return alternateDataType == long.class ? (ColumnSource<ALTERNATE_DATA_TYPE>) new AsLong() :
                super.doReinterpret(alternateDataType);
    }

    @Override
    public void releaseCachedResources() {
        super.releaseCachedResources();
        dictionaryColumn.releaseCachedResources();
    }

    class AsLong extends RegionedColumnSourceBase<Long, Attributes.DictionaryKeys, ColumnRegionReferencing<Attributes.DictionaryKeys, ColumnRegionInt<Attributes.DictionaryKeys>>>
            implements ColumnSourceGetDefaults.ForLong, ColumnRegionReferencingImpl.Converter<Attributes.DictionaryKeys>  {

        AsLong() {
            super(long.class);
        }

        @Override
        <OTHER_REGION_TYPE> int addRegionForUnitTests(OTHER_REGION_TYPE region) {
            return RegionedColumnSourceObjectWithDictionary.this.addRegionForUnitTests(region);
        }

        @NotNull
        @Override
        ColumnRegionReferencing<Attributes.DictionaryKeys, ColumnRegionInt<Attributes.DictionaryKeys>> getNullRegion() {
            return RegionedColumnSourceObjectWithDictionary.this.getNullRegion();
        }

        @Override
        public int addRegion(@NotNull ColumnDefinition<?> columnDefinition, @NotNull ColumnLocation<?> columnLocation) {
            return RegionedColumnSourceObjectWithDictionary.this.addRegion(columnDefinition, columnLocation);
        }

        @Override
        public int getRegionCount() {
            return RegionedColumnSourceObjectWithDictionary.this.getRegionCount();
        }

        @Override
        public ColumnRegionReferencing<Attributes.DictionaryKeys, ColumnRegionInt<Attributes.DictionaryKeys>> getRegion(int regionIndex) {
            return RegionedColumnSourceObjectWithDictionary.this.getRegion(regionIndex);
        }

        @Override
        public long getLong(long elementIndex) {
            if (elementIndex != NULL_KEY) {
                int regionIndex = RegionedPageStore.getRegionIndex(elementIndex);
                int key = RegionedColumnSourceObjectWithDictionary.this.getRegion(regionIndex).getReferencedRegion().getInt(elementIndex);
                if (key != NULL_INT) {
                    return RegionedPageStore.getElementIndex(regionIndex, key);
                }
            }

            return NULL_LONG;
        }

        @Override
        public void convertRegion(WritableChunk<? super Attributes.DictionaryKeys> destination, Chunk<? extends Attributes.DictionaryKeys> source, OrderedKeys orderedKeys) {
            WritableLongChunk<? super Attributes.DictionaryKeys> longChunk = destination.asWritableLongChunk();
            IntChunk<? extends Attributes.DictionaryKeys> intChunk = source.asIntChunk();

            final int regionIndex = RegionedPageStore.getRegionIndex(orderedKeys.firstKey());

            final int size = longChunk.size();
            final int length = intChunk.size();

            for (int i = 0; i < length; ++i) {
                int key = intChunk.get(i);
                longChunk.set(size + i, key == NULL_INT ? NULL_LONG : RegionedPageStore.getElementIndex(regionIndex, key));
            }
            longChunk.setSize(size + length);
        }

        @Override
        public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return alternateDataType == RegionedColumnSourceObjectWithDictionary.this.getNativeType();
        }

        @Override
        protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            //noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) RegionedColumnSourceObjectWithDictionary.this;
        }

        @Override
        public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
            return RegionedColumnSourceObjectWithDictionary.this.makeFillContext(this, chunkCapacity, sharedContext);
        }

        @Override @OverridingMethodsMustInvokeSuper
        public void releaseCachedResources() {
            // We are a reinterpreted column of RegionedColumnSourceObjectReferencing.this, so if we're asked to release
            // our resources, release the real resources in the underlying column.
            super.releaseCachedResources();
            RegionedColumnSourceObjectWithDictionary.this.releaseCachedResources();
        }
    }

    @Override
    public QueryTable getStaticSymbolTable(@NotNull Index sourceIndex, boolean useLookupCaching) {
        final Index.SequentialBuilder symbolTableIndexBuilder = Index.FACTORY.getSequentialBuilder();

        try (Index.SearchIterator sourceIndexIterator = sourceIndex.searchIterator()) {
            for (int regionIndex = 0; sourceIndexIterator.advance(RegionedPageStore.getFirstElementIndex(regionIndex)); ++regionIndex) {
                long sourceElementIndex = sourceIndexIterator.currentValue();
                regionIndex = RegionedPageStore.getRegionIndex(sourceElementIndex);
                ColumnRegionObject<T, Attributes.Values> region = dictionaryColumn.getRegion(regionIndex);

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
            // TODO: Update and use io.deephaven.db.tables.verify.TableAssertions.assertAppendOnly(java.lang.String, io.deephaven.db.tables.Table) ?
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
                for (int regionIndex = 0; sourceAddedIterator.advance(RegionedPageStore.getFirstElementIndex(regionIndex)); ++regionIndex) {
                    final long sourceElementIndex = sourceAddedIterator.currentValue();
                    regionIndex = RegionedPageStore.getRegionIndex(sourceElementIndex);
                    ColumnRegionObject<T, Attributes.Values> region = dictionaryColumn.getRegion(regionIndex);

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
