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
 * {@link ColumnRegionDouble} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionDouble<ATTR extends Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionDouble<ATTR>>
        implements ColumnRegionDouble<ATTR> {

    DeferredColumnRegionDouble(final long pageMask, @NotNull Supplier<ColumnRegionDouble<ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public double getDouble(final long elementIndex) {
        return getResultRegion().getDouble(elementIndex);
    }

    @Override
    public double getDouble(@NotNull final FillContext context, final long elementIndex) {
        return getResultRegion().getDouble(context, elementIndex);
    }
}
