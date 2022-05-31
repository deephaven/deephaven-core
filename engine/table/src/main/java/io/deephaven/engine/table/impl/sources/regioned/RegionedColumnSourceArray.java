package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Require;

import javax.annotation.OverridingMethodsMustInvokeSuper;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

/**
 * Base class for all {@link RegionedColumnSource} implementations with column regions stored in an array.
 */
abstract class RegionedColumnSourceArray<DATA_TYPE, ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>>
        extends RegionedColumnSourceBase<DATA_TYPE, ATTR, REGION_TYPE>
        implements MakeRegion<ATTR, REGION_TYPE> {

    @FunctionalInterface
    interface MakeDeferred<ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>> {
        REGION_TYPE make(final long pageMask, Supplier<REGION_TYPE> supplier);
    }

    private final REGION_TYPE nullRegion;
    private final MakeDeferred<ATTR, REGION_TYPE> makeDeferred;
    private volatile int regionCount = 0;
    private volatile REGION_TYPE[] regions;

    @SuppressWarnings("rawtypes")
    private static final ColumnRegion[] EMPTY = new ColumnRegion[0];

    private static <ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>> REGION_TYPE[] allocateRegionArray(
            int length) {
        // noinspection unchecked
        return (REGION_TYPE[]) (length == 0 ? EMPTY : new ColumnRegion[length]);
    }

    /**
     * Construct a {@code RegionedColumnSource} which is an array of references to {@code ColumnRegion}s.
     *
     * @param nullRegion A ColumnRegion to be used when the actual region doesn't exist, which returns the correct null
     *        values for that region.
     * @param type The type of the column.
     * @param componentType The component type in case the main type is a Vector
     * @param makeDeferred A function which creates the correct deferred region for this ColumnSource. If you don't want
     *        any deferred regions then use Supplier::get.
     */
    RegionedColumnSourceArray(@NotNull final REGION_TYPE nullRegion,
            @NotNull final Class<DATA_TYPE> type,
            @Nullable final Class componentType,
            @NotNull final MakeDeferred<ATTR, REGION_TYPE> makeDeferred) {
        super(type, componentType);
        this.nullRegion = nullRegion;
        this.makeDeferred = makeDeferred;
        regions = allocateRegionArray(0);
    }

    /**
     * Delegate to {@link #RegionedColumnSourceArray(ColumnRegion, Class, Class, MakeDeferred)} with null component
     * type.
     *
     * @param nullRegion A ColumnRegion to be used when the actual region doesn't exist, which returns the correct null
     *        values for that region.
     * @param type The type of the column.
     * @param makeDeferred A function which creates the correct deferred region for this ColumnSource. If you don't want
     *        any deferred regions then use Supplier::get.
     */
    RegionedColumnSourceArray(@NotNull final REGION_TYPE nullRegion,
            @NotNull final Class<DATA_TYPE> type,
            @NotNull final MakeDeferred<ATTR, REGION_TYPE> makeDeferred) {
        this(nullRegion, type, null, makeDeferred);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public synchronized int addRegion(@NotNull final ColumnDefinition<?> columnDefinition,
            @NotNull final ColumnLocation columnLocation) {
        maybeExtendRegions();
        final int regionIndex = regionCount;
        regions[regionIndex] = makeDeferred.make(PARAMETERS.regionMask,
                () -> updateRegion(regionIndex, makeRegion(columnDefinition, columnLocation, regionIndex)));
        return regionCount++;
    }

    /**
     * <p>
     * Add a pre-constructed region without going through the abstract factory method
     * {@link #makeRegion(ColumnDefinition, ColumnLocation, int)}.
     * <p>
     * <em>This method is for unit testing purposes only!</em>
     *
     * @param region The region to add
     * @return The index assigned to the added region
     */
    @Override
    @TestUseOnly
    synchronized <OTHER_REGION_TYPE> int addRegionForUnitTests(final OTHER_REGION_TYPE region) {
        Require.neqNull(region, "region");
        maybeExtendRegions();
        // noinspection unchecked
        regions[regionCount] = region == null ? nullRegion : (REGION_TYPE) region;
        return regionCount++;
    }

    /**
     * Re-allocate the {@code regions} array if needed.
     */
    private void maybeExtendRegions() {
        if (regionCount < regions.length) {
            return;
        }
        if (regionCount == MAXIMUM_REGION_COUNT) {
            throw new IllegalStateException("Cannot add another region to " + this + ", maximum region count "
                    + MAXIMUM_REGION_COUNT + " reached");
        }
        final int newLength = Math.min(Math.max(regions.length * 2, regionCount + 1), MAXIMUM_REGION_COUNT);
        final REGION_TYPE[] newRegions = allocateRegionArray(newLength);
        System.arraycopy(regions, 0, newRegions, 0, regionCount);
        regions = newRegions;
    }

    /**
     * Update the region at a given index in this regioned column source. This is intended to be used by the region
     * suppliers in DeferredColumnRegion implementations.
     *
     * @param regionIndex The region index
     * @param region The new column region
     * @return The new column region, for method-chaining purposes
     */
    @NotNull
    private synchronized REGION_TYPE updateRegion(final int regionIndex, final REGION_TYPE region) {
        return regions[regionIndex] = region == null ? nullRegion : region;
    }

    @Override
    public final int getRegionCount() {
        return regionCount;
    }

    @Override
    public REGION_TYPE getRegion(final int regionIndex) {
        return regions[regionIndex];
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void releaseCachedResources() {
        super.releaseCachedResources();
        for (int ri = 0; ri < getRegionCount(); ++ri) {
            getRegion(ri).releaseCachedResources();
        }
    }

    @NotNull
    public final REGION_TYPE getNullRegion() {
        return nullRegion;
    }
}
