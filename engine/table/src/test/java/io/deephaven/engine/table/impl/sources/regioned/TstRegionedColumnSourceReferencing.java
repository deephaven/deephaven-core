//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;

/**
 * Base class for testing {@link RegionedColumnSourceArray} implementations.
 */
public abstract class TstRegionedColumnSourceReferencing<DATA_TYPE, ATTR extends Values, NATIVE_REGION_TYPE extends ColumnRegion<ATTR>>
        extends
        TstRegionedColumnSourcePrimitive<DATA_TYPE, ATTR, NATIVE_REGION_TYPE, ColumnRegionReferencing<ATTR, NATIVE_REGION_TYPE>> {

    TstRegionedColumnSourceReferencing(Class<?> regionTypeClass) {
        super(regionTypeClass);
    }

    @Override
    NATIVE_REGION_TYPE doLookupRegion(long index) {
        return SUT.lookupRegion(index).getReferencedRegion();
    }
}
