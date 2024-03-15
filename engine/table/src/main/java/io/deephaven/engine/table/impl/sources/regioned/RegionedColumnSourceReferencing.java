//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * Base class for a column of {@code DATA_TYPE} which is a wrapped {@code NATIVE_DATA_TYPE}. The column owns the
 * underlying native column and its resources.
 */
abstract class RegionedColumnSourceReferencing<DATA_TYPE, ATTR extends Values, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE extends ColumnRegion<ATTR>>
        extends RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>>
        implements ColumnRegionReferencingImpl.Converter<ATTR> {

    @NotNull
    private final ColumnRegionReferencing.Null<ATTR, NATIVE_REGION_TYPE> nullRegion;

    @NotNull
    private final RegionedColumnSourceBase<NATIVE_DATA_TYPE, ATTR, NATIVE_REGION_TYPE> nativeSource;

    RegionedColumnSourceReferencing(@NotNull final NATIVE_REGION_TYPE nullRegion,
            @NotNull Class<DATA_TYPE> type,
            @NotNull RegionedColumnSourceBase<NATIVE_DATA_TYPE, ATTR, NATIVE_REGION_TYPE> nativeSource) {
        super(type);
        this.nullRegion = new ColumnRegionReferencing.Null<>(nullRegion);
        this.nativeSource = nativeSource;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return nativeSource.getType() == alternateDataType || nativeSource.allowsReinterpret(alternateDataType);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        // noinspection unchecked
        return nativeSource.getType() == alternateDataType ? (ColumnSource<ALTERNATE_DATA_TYPE>) nativeSource
                : nativeSource.reinterpret(alternateDataType);
    }

    @Override
    public ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE> getRegion(int regionIndex) {
        final NATIVE_REGION_TYPE nativeRegion = nativeSource.getRegion(regionIndex);
        if (nativeRegion == nativeSource.getNullRegion()) {
            return nullRegion;
        }

        return new ColumnRegionReferencingImpl<>(nativeRegion);
    }

    @Override
    public int getRegionCount() {
        return nativeSource.getRegionCount();
    }

    @Override
    public int addRegion(@NotNull ColumnDefinition<?> columnDefinition, @NotNull ColumnLocation columnLocation) {
        return nativeSource.addRegion(columnDefinition, columnLocation);
    }

    @Override
    <OTHER_REGION_TYPE> int addRegionForUnitTests(OTHER_REGION_TYPE region) {
        return nativeSource.addRegionForUnitTests(region);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return new ColumnRegionReferencingImpl.FillContext<>(nativeSource, this, chunkCapacity, sharedContext);
    }

    @NotNull
    @Override
    public ColumnRegionReferencing.Null<ATTR, NATIVE_REGION_TYPE> getNullRegion() {
        return nullRegion;
    }

    @NotNull
    public RegionedColumnSourceBase<NATIVE_DATA_TYPE, ATTR, NATIVE_REGION_TYPE> getNativeSource() {
        return nativeSource;
    }
}
