//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.JMockRule;
import io.deephaven.chunk.attributes.Values;
import org.junit.Rule;
import org.junit.Test;

import java.util.function.Supplier;

/**
 * Base class for testing {@link ColumnRegion} implementations.
 */

abstract class TstColumnRegionPrimative<REGION_TYPE extends ColumnRegion<Values>> {

    @Rule
    public final JMockRule jmock = new JMockRule();

    REGION_TYPE SUT;

    @Test
    public abstract void testGet();

    static abstract class Deferred<REGION_TYPE extends ColumnRegion<Values>>
            extends TstColumnRegionPrimative<REGION_TYPE> {

        Supplier<REGION_TYPE> regionSupplier;
    }
}
