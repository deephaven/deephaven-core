/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DeferredColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionLong} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionLong<ATTR extends Attributes.Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionLong<ATTR>>
        implements ColumnRegionLong<ATTR> {

    DeferredColumnRegionLong(final long pageMask, @NotNull Supplier<ColumnRegionLong<ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public long getLong(final long elementIndex) {
        return getResultRegion().getLong(elementIndex);
    }

    @Override
    public long getLong(@NotNull final FillContext context, final long elementIndex) {
        return getResultRegion().getLong(context, elementIndex);
    }
}
