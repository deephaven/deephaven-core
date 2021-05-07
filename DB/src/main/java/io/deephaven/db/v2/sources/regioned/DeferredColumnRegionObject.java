/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit DeferredColumnRegionChar and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionShort} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionObject<T, ATTR extends Attributes.Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionObject<T, ATTR>>
        implements ColumnRegionObject<T, ATTR> {

    DeferredColumnRegionObject(@NotNull Supplier<ColumnRegionObject<T, ATTR>> resultRegionFactory) {
        super(resultRegionFactory);
    }

    @Override
    public T getObject(long elementIndex) {
        return getResultRegion().getObject(elementIndex);
    }

    @Override
    public T getObject(@NotNull FillContext context, long elementIndex) {
        return getResultRegion().getObject(context, elementIndex);
    }
}
