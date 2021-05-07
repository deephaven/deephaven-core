package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.sources.chunk.Attributes;
import org.junit.Before;

import java.lang.reflect.Array;

public abstract class TstRegionedColumnSourceReferenced<DATA_TYPE, ATTR extends Attributes.Values, REGION_TYPE extends ColumnRegion<ATTR>,
        REF_DATA_TYPE, REF_ATTR extends Attributes.Values, REF_REGION_TYPE extends ColumnRegion<REF_ATTR>>
        extends TstRegionedColumnSourceReferencing<DATA_TYPE, ATTR, REGION_TYPE> {

    REF_REGION_TYPE[] cr_r;
    RegionedColumnSourceBase<REF_DATA_TYPE, REF_ATTR, REF_REGION_TYPE> SUT_R;

    private final Class<?> refRegionTypeClass;

    TstRegionedColumnSourceReferenced(Class<?> regionTypeClass,
            Class<?> refRegionTypeClass) {
        super(regionTypeClass);
        this.refRegionTypeClass = refRegionTypeClass;
    }

    @Override
    void fillRegions() {
        super.fillRegions();
        for (REF_REGION_TYPE region : cr_r) {
            SUT_R.addRegionForUnitTests(region);
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();

        //noinspection unchecked
        cr_r = (REF_REGION_TYPE[]) Array.newInstance(refRegionTypeClass, 10);
        for (int cri = 0; cri < cr_r.length; ++cri) {
            //noinspection unchecked
            cr_r[cri] = (REF_REGION_TYPE) mock(refRegionTypeClass, "CR_R_" + cri);
        }

        // Sub-classes are responsible for setting up SUT and SUT_R
    }
}
