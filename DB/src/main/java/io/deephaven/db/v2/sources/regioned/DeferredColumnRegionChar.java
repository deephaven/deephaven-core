package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionChar} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionChar<ATTR extends Attributes.Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionChar<ATTR>>
        implements ColumnRegionChar<ATTR> {

    DeferredColumnRegionChar(final long pageMask, @NotNull Supplier<ColumnRegionChar<ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public char getChar(final long elementIndex) {
        return getResultRegion().getChar(elementIndex);
    }

    @Override
    public char getChar(@NotNull final FillContext context, final long elementIndex) {
        return getResultRegion().getChar(context, elementIndex);
    }
}
