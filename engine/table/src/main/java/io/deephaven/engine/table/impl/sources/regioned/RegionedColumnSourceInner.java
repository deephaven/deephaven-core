package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * <p>
 * Base class for column source which reaches its regions by reaching into the regions of an outer column source. These
 * derive directly from {@link RegionedColumnSourceBase}, and thus don't maintain their own array of regions.
 * </p>
 *
 * <p>
 * Extending classes will typically override {@link RegionedPageStore#getRegion(int)} to reach into the outer column
 * source.
 * </p>
 */
abstract class RegionedColumnSourceInner<DATA_TYPE, ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>, OUTER_DATA_TYPE, OUTER_REGION_TYPE extends ColumnRegion<ATTR>>
        extends RegionedColumnSourceBase<DATA_TYPE, ATTR, REGION_TYPE> {

    private final RegionedColumnSourceBase<OUTER_DATA_TYPE, ATTR, OUTER_REGION_TYPE> outerColumnSource;

    RegionedColumnSourceInner(@NotNull Class<DATA_TYPE> type,
            RegionedColumnSourceBase<OUTER_DATA_TYPE, ATTR, OUTER_REGION_TYPE> outerColumnSource) {
        super(type);
        this.outerColumnSource = outerColumnSource;
    }

    @Override
    final <OTHER_REGION_TYPE> int addRegionForUnitTests(OTHER_REGION_TYPE region) {
        return outerColumnSource.addRegionForUnitTests(region);
    }

    @Override
    public final int addRegion(@NotNull ColumnDefinition<?> columnDefinition, @NotNull ColumnLocation columnLocation) {
        return outerColumnSource.addRegion(columnDefinition, columnLocation);
    }

    @Override
    public final int getRegionCount() {
        return outerColumnSource.getRegionCount();
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        // We are a reinterpreted column of the outer column source, so if we're asked to release our resources, release
        // the real resources in the underlying column.
        super.releaseCachedResources();
        getOuterColumnSource().releaseCachedResources();
    }

    final RegionedColumnSourceBase<OUTER_DATA_TYPE, ATTR, OUTER_REGION_TYPE> getOuterColumnSource() {
        return outerColumnSource;
    }
}
