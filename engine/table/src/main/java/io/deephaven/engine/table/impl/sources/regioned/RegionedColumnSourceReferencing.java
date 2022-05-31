package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * Base class for a column of {@code DATA_TYPE} which is a wrapped {@code NATIVE_DATA_TYPE}. The column owns the
 * underlying native column and its resources.
 */
abstract class RegionedColumnSourceReferencing<DATA_TYPE, ATTR extends Values, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE extends ColumnRegion<ATTR>>
        extends RegionedColumnSourceArray<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>>
        implements ColumnRegionReferencingImpl.Converter<ATTR> {

    @FunctionalInterface
    interface NativeSourceCreator<DATA_TYPE, ATTR extends Values, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE extends ColumnRegion<ATTR>> {
        NativeColumnSource<DATA_TYPE, ATTR, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE> create(
                RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>> outerSource);
    }

    @NotNull
    private final NativeColumnSource<DATA_TYPE, ATTR, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE> nativeSource;

    RegionedColumnSourceReferencing(@NotNull final NATIVE_REGION_TYPE nullRegion,
            @NotNull Class<DATA_TYPE> type,
            @NotNull NativeSourceCreator<DATA_TYPE, ATTR, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE> nativeSourceCreator) {
        super(new ColumnRegionReferencing.Null<>(nullRegion), type, DeferredColumnRegionReferencing::new);
        nativeSource = nativeSourceCreator.create(this);
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
    @Nullable
    public ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE> makeRegion(@NotNull ColumnDefinition<?> columnDefinition,
            @NotNull ColumnLocation columnLocation, int regionIndex) {
        NATIVE_REGION_TYPE nativeRegionType = nativeSource.makeRegion(columnDefinition, columnLocation, regionIndex);
        return nativeRegionType == null ? null : new ColumnRegionReferencingImpl<>(nativeRegionType);
    }

    final ChunkSource.FillContext makeFillContext(ColumnRegionReferencing.Converter<ATTR> converter, int chunkCapacity,
            SharedContext sharedContext) {
        return new ColumnRegionReferencingImpl.FillContext<>(nativeSource, converter, chunkCapacity, sharedContext);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return makeFillContext(this, chunkCapacity, sharedContext);
    }

    abstract static class NativeColumnSource<DATA_TYPE, ATTR extends Values, NATIVE_DATA_TYPE, NATIVE_REGION_TYPE extends ColumnRegion<ATTR>>
            extends
            RegionedColumnSourceInner<NATIVE_DATA_TYPE, ATTR, NATIVE_REGION_TYPE, DATA_TYPE, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>>
            implements MakeRegion<ATTR, NATIVE_REGION_TYPE> {

        NativeColumnSource(@NotNull Class<NATIVE_DATA_TYPE> type,
                RegionedColumnSourceBase<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>> outerColumnSource) {
            super(type, outerColumnSource);
        }

        @Override
        @NotNull
        NATIVE_REGION_TYPE getNullRegion() {
            return getOuterColumnSource().getNullRegion().getReferencedRegion();
        }

        @Override
        public NATIVE_REGION_TYPE getRegion(int regionIndex) {
            return getOuterColumnSource().getRegion(regionIndex).getReferencedRegion();
        }

        @Override
        public final <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            return getOuterColumnSource().getType() == alternateDataType;
        }

        @Override
        protected final <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(
                @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
            // noinspection unchecked
            return (ColumnSource<ALTERNATE_DATA_TYPE>) getOuterColumnSource();
        }
    }
}
