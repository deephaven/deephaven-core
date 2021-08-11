package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionChar} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
class DeferredColumnRegionByte<ATTR extends Attributes.Any> extends DeferredColumnRegionBase<ATTR, ColumnRegionByte<ATTR>>
        implements ColumnRegionByte<ATTR> {

    DeferredColumnRegionByte(final long pageMask, @NotNull Supplier<ColumnRegionByte<ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public byte getByte(long elementIndex) {
        return getResultRegion().getByte(elementIndex);
    }

    @Override
    public byte getByte(@NotNull FillContext context, long elementIndex) {
        return getResultRegion().getByte(context, elementIndex);
    }

    public byte[] getBytes(long firstElementIndex, @NotNull byte[] destination, int destinationOffset, int length) {
        return getResultRegion().getBytes(firstElementIndex, destination, destinationOffset, length);
    }
}
