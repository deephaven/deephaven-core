package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.table.impl.chunkattributes.DictionaryKeys;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionShort} implementation for deferred regions, i.e. regions that will be properly constructed on
 * first access.
 */
public class DeferredColumnRegionObject<DATA_TYPE, ATTR extends Any>
        extends DeferredColumnRegionBase<ATTR, ColumnRegionObject<DATA_TYPE, ATTR>>
        implements ColumnRegionObject<DATA_TYPE, ATTR> {

    DeferredColumnRegionObject(final long pageMask, @NotNull final Supplier<ColumnRegionObject<DATA_TYPE, ATTR>> resultRegionFactory) {
        super(pageMask, resultRegionFactory);
    }

    @Override
    public DATA_TYPE getObject(final long elementIndex) {
        return getResultRegion().getObject(elementIndex);
    }

    @Override
    public DATA_TYPE getObject(@NotNull final FillContext context, final long elementIndex) {
        return getResultRegion().getObject(context, elementIndex);
    }

    @Override
    public RegionVisitResult supportsDictionaryFormat(@NotNull final RowSet.SearchIterator keysToVisit) {
        return getResultRegion().supportsDictionaryFormat(keysToVisit);
    }

    @Override
    public boolean gatherDictionaryValuesRowSet(@NotNull final RowSet.SearchIterator keysToVisit,
                                                @NotNull final RowSequence.Iterator knownKeys,
                                                @NotNull final RowSetBuilderSequential sequentialBuilder) {
        return getResultRegion().gatherDictionaryValuesRowSet(keysToVisit, knownKeys, sequentialBuilder);
    }

    @Override
    public ColumnRegionLong<DictionaryKeys> getDictionaryKeysRegion() {
        return getResultRegion().getDictionaryKeysRegion();
    }

    @Override
    public ColumnRegionObject<DATA_TYPE, ATTR> getDictionaryValuesRegion() {
        return getResultRegion().getDictionaryValuesRegion();
    }
}
