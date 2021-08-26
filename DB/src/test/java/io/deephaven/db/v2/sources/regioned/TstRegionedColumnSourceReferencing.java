/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.junit.Before;

import java.lang.reflect.Array;

/**
 * Base class for testing {@link RegionedColumnSourceArray} implementations.
 */
@SuppressWarnings({"AnonymousInnerClassMayBeStatic"})
public abstract class TstRegionedColumnSourceReferencing<DATA_TYPE, ATTR extends Attributes.Values, NATIVE_REGION_TYPE extends ColumnRegion<ATTR>>
    extends
    TstRegionedColumnSourcePrimitive<DATA_TYPE, ATTR, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>> {

    NATIVE_REGION_TYPE[] cr_n;

    private final Class<?> nativeRegionTypeClass;

    TstRegionedColumnSourceReferencing(Class<?> nativeRegionTypeClass) {
        super(ColumnRegionReferencing.class);
        this.nativeRegionTypeClass = nativeRegionTypeClass;
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // noinspection unchecked
        cr_n = (NATIVE_REGION_TYPE[]) Array.newInstance(nativeRegionTypeClass, 10);
        for (int cri = 0; cri < cr.length; ++cri) {
            // noinspection unchecked
            cr_n[cri] = (NATIVE_REGION_TYPE) mock(nativeRegionTypeClass, "CR_N_" + cri);
        }

        // Sub-classes are responsible for setting up SUT.
    }
}
