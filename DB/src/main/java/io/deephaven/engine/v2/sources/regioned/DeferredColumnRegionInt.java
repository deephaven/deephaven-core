/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DeferredColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.sources.regioned;

import io.deephaven.engine.structures.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionInt} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionInt<ATTR extends Attributes.Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionInt<ATTR>>
        implements ColumnRegionInt<ATTR> {

    DeferredColumnRegionInt(final long pageMask, @NotNull Supplier<ColumnRegionInt<ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public int getInt(final long elementIndex) {
        return getResultRegion().getInt(elementIndex);
    }

    @Override
    public int getInt(@NotNull final FillContext context, final long elementIndex) {
        return getResultRegion().getInt(context, elementIndex);
    }
}
