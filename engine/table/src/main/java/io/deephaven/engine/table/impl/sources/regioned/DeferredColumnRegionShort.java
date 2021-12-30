/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DeferredColumnRegionChar and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionShort} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionShort<ATTR extends Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionShort<ATTR>>
        implements ColumnRegionShort<ATTR> {

    DeferredColumnRegionShort(final long pageMask, @NotNull Supplier<ColumnRegionShort<ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public short getShort(final long elementIndex) {
        return getResultRegion().getShort(elementIndex);
    }

    @Override
    public short getShort(@NotNull final FillContext context, final long elementIndex) {
        return getResultRegion().getShort(context, elementIndex);
    }
}
