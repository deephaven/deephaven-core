package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.sources.AbstractDeferredGroupingColumnSource;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Partial implementation of {@link RegionedColumnSource} for array-backed and delegating implementations to extend.
 */
abstract class RegionedColumnSourceBase<DATA_TYPE, ATTR extends Values, REGION_TYPE extends ColumnRegion<ATTR>>
        extends AbstractDeferredGroupingColumnSource<DATA_TYPE>
        implements RegionedPageStore<Values, ATTR, REGION_TYPE>, RegionedColumnSource<DATA_TYPE> {

    static final Parameters PARAMETERS;
    static {
        PARAMETERS =
                new RegionedPageStore.Parameters(Long.MAX_VALUE, MAXIMUM_REGION_COUNT, REGION_CAPACITY_IN_ELEMENTS);
        Assert.eq(PARAMETERS.regionMask, "parameters.regionMask", ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK,
                "ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK");
        Assert.eq(PARAMETERS.regionMaskNumBits, "parameters.regionMaskNumBits", SUB_REGION_ROW_INDEX_ADDRESS_BITS,
                "SUB_REGION_ELEMENT_INDEX_ADDRESS_BITS");
    }

    RegionedColumnSourceBase(@NotNull final Class<DATA_TYPE> type, @Nullable final Class<?> componentType) {
        super(type, componentType);
    }

    RegionedColumnSourceBase(@NotNull final Class<DATA_TYPE> type) {
        this(type, null);
    }

    @Override
    public final Parameters parameters() {
        return PARAMETERS;
    }

    /**
     * Use the more efficient fill chunk implementation, rather than the default which uses get().
     */
    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        RegionedPageStore.super.fillChunk(context, destination, rowSequence);
    }

    /**
     * We are immutable, so stick with the efficient fill chunk even when getting prev.
     */
    @Override
    public void fillPrevChunk(@NotNull FillContext context,
            @NotNull WritableChunk<? super Values> destination, @NotNull RowSequence rowSequence) {
        fillChunk(context, destination, rowSequence);
    }

    /**
     * <p>
     * Add a pre-constructed region without going through the abstract factory method.
     * <p>
     * <em>This method is for unit testing purposes only!</em>
     *
     * @param region The region to add
     * @return The index assigned to the added region
     */
    @SuppressWarnings("UnusedReturnValue")
    @TestUseOnly
    abstract <OTHER_REGION_TYPE> int addRegionForUnitTests(final OTHER_REGION_TYPE region);

    /**
     * @return the region which represents null for this column source.
     */
    @NotNull
    abstract REGION_TYPE getNullRegion();
}
