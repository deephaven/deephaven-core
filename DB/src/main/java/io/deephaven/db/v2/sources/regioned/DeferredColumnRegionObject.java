package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionShort} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionObject<DATA_TYPE, ATTR extends Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
        implements ColumnRegionObject<DATA_TYPE, ATTR> {

    DeferredColumnRegionObject(final long pageMask, @NotNull Supplier<ColumnRegionObject<DATA_TYPE, ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public DATA_TYPE getObject(long elementIndex) {
        return getResultRegion().getObject(elementIndex);
    }

    @Override
    public DATA_TYPE getObject(@NotNull FillContext context, long elementIndex) {
        return getResultRegion().getObject(context, elementIndex);
    }

    @Override
    public boolean supportsDictionaryFormat(@NotNull final OrderedKeys.Iterator remainingKeys, final boolean failFast) {
        return getResultRegion().supportsDictionaryFormat(remainingKeys, failFast);
    }

    @Override
    public ColumnRegionLong<Attributes.DictionaryKeys> getDictionaryKeysRegion() {
        return getResultRegion().getDictionaryKeysRegion();
    }

    @Override
    public ColumnRegionObject<DATA_TYPE, ATTR> getDictionaryValuesRegion() {
        return getResultRegion().getDictionaryValuesRegion();
    }
}
