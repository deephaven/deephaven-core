//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.page.Page;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.InvalidatedRegionException;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.table.impl.sources.IntegerSingleValueSource;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/**
 * Tests for {@link DeferredColumnRegionBase}.
 */
public class TestDeferredColumnRegion {

    private static final long REGION_MASK = RegionedColumnSourceBase.PARAMETERS.regionMask;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * A region whose reads fail once the region is invalidated, as the reads of a region backed by a location do.
     */
    private static final class InvalidatableRegion extends GenericColumnRegionBase<Values>
            implements ColumnRegionInt<Values>, Page.WithDefaults<Values> {

        private InvalidatableRegion() {
            super(REGION_MASK);
        }

        @Override
        public int getInt(final long elementIndex) {
            throwIfInvalidated();
            return (int) elementIndex;
        }

        @Override
        public void fillChunkAppend(
                @NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            throwIfInvalidated();
            final WritableIntChunk<? super Values> intDestination = destination.asWritableIntChunk();
            rowSequence.forAllRowKeys(key -> intDestination.add((int) key));
        }
    }

    private static WhereFilter initializedFilter(final String expression) {
        final WhereFilter filter = WhereFilterFactory.getExpression(expression);
        filter.init(TableDefinition.of(ColumnDefinition.ofInt("A")));
        return filter;
    }

    private static RegionedPushdownFilterContextImpl makeFilterContext(final WhereFilter filter) {
        return new RegionedPushdownFilterContextImpl(
                filter, List.of(new IntegerSingleValueSource()), List.of(ColumnDefinition.ofInt("A")), Map.of());
    }

    /**
     * Make a deferred region whose factory holds the only strong reference to {@code location}, as the factory for a
     * column source's region holds the column location.
     */
    private static DeferredColumnRegionInt<Values> makeRegionHolding(final Object location) {
        return new DeferredColumnRegionInt<>(REGION_MASK, () -> {
            Objects.requireNonNull(location);
            return new InvalidatableRegion();
        });
    }

    /**
     * An invalidation that arrives before the first access must fail every later read, and must not construct the
     * region for the removed location.
     */
    @Test
    public void testInvalidateBeforeFirstAccess() {
        final AtomicInteger factoryCalls = new AtomicInteger();
        final DeferredColumnRegionInt<Values> region = new DeferredColumnRegionInt<>(REGION_MASK, () -> {
            factoryCalls.incrementAndGet();
            return new InvalidatableRegion();
        });
        region.invalidate();

        assertThrows(InvalidatedRegionException.class, () -> region.getInt(7));

        try (final WritableIntChunk<Values> destination = WritableIntChunk.makeWritableChunk(4);
                final RowSequence rows = RowSequenceFactory.forRange(0, 3)) {
            assertThrows(InvalidatedRegionException.class,
                    () -> region.fillChunk(ChunkSource.DEFAULT_FILL_INSTANCE, destination, rows));
        }

        final WhereFilter filter = initializedFilter("A == 5");
        try (final RegionedPushdownFilterContextImpl context = makeFilterContext(filter);
                final RowSet selection = RowSetFactory.flat(10)) {
            assertThrows(InvalidatedRegionException.class,
                    () -> region.pushdownFilter(filter, selection, false, context, Long.MAX_VALUE,
                            new ImmediateJobScheduler(),
                            result -> {
                                result.close();
                                fail("a pushdown against an invalidated region must fail");
                            },
                            e -> fail("unexpected failure " + e)));
        }

        assertEquals(0, factoryCalls.get());
    }

    /**
     * An invalidation that arrives before the first access must release the factory, and with the factory the removed
     * location that the factory references.
     */
    @Test
    public void testInvalidateBeforeFirstAccessReleasesFactory() {
        Object location = new Object();
        final WeakReference<Object> locationReference = new WeakReference<>(location);
        final DeferredColumnRegionInt<Values> region = makeRegionHolding(location);
        location = null;

        region.invalidate();

        for (int attempt = 0; attempt < 10 && locationReference.get() != null; ++attempt) {
            System.gc();
        }
        assertNull(locationReference.get());
        // The region must still fail reads, rather than reach the released factory.
        assertThrows(InvalidatedRegionException.class, () -> region.getInt(7));
    }

    /**
     * An invalidation that arrives after the first access must reach the materialized region.
     */
    @Test
    public void testInvalidateAfterFirstAccess() {
        final DeferredColumnRegionInt<Values> region =
                new DeferredColumnRegionInt<>(REGION_MASK, InvalidatableRegion::new);
        assertEquals(7, region.getInt(7));

        region.invalidate();

        assertThrows(InvalidatedRegionException.class, () -> region.getInt(7));
    }
}
