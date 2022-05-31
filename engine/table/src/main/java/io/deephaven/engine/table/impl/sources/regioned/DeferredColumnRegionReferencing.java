package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionReferencing} implementation for deferred regions, i.e. regions that will be properly constructed
 * on first access.
 */
public class DeferredColumnRegionReferencing<ATTR extends Any, REFERENCED_COLUMN_REGION extends ColumnRegion<ATTR>>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION>>
        implements ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION> {

    DeferredColumnRegionReferencing(final long pageMask,
            @NotNull Supplier<ColumnRegionReferencing<ATTR, REFERENCED_COLUMN_REGION>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @NotNull
    @Override
    public REFERENCED_COLUMN_REGION getReferencedRegion() {
        return getResultRegion().getReferencedRegion();
    }

}
