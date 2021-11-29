/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import java.util.function.Supplier;

/**
 * Base class for testing {@link ColumnRegion} implementations.
 */
@SuppressWarnings("JUnit4AnnotatedMethodInJUnit3TestCase")
abstract class TstColumnRegionPrimative<REGION_TYPE extends ColumnRegion<Values>> extends BaseArrayTestCase {

    REGION_TYPE SUT;

    @Test
    public abstract void testGet();

    static abstract class Deferred<REGION_TYPE extends ColumnRegion<Values>>
            extends TstColumnRegionPrimative<REGION_TYPE> {

        Supplier<REGION_TYPE> regionSupplier;
    }
}
