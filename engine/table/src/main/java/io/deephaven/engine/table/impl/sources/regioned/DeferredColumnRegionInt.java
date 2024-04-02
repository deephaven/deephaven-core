//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit DeferredColumnRegionChar and run "./gradlew replicateRegionsAndRegionedSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * {@link ColumnRegionInt} implementation for deferred regions, i.e. regions that will be properly constructed on first
 * access.
 */
public class DeferredColumnRegionInt<ATTR extends Any>
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
