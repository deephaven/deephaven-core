//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.modelfarm.util;

import io.deephaven.base.testing.Asserts;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.*;
import io.deephaven.engine.util.TableTools;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class TestModelFarmUtils extends RefreshingTableTestCase {

    @Test
    public void testRequireTable() {
        final Table t = TableTools.emptyTable(5).updateView("A=(int)i", "B=(long)i", "C=(double)i");

        ModelFarmUtils.requireTable("TABLENAME", t, new String[] {"A", "C", "B"},
                new Class[] {int.class, double.class, long.class});

        try {
            ModelFarmUtils.requireTable("TABLENAME", t, new String[] {"A", "X", "B"},
                    new Class[] {int.class, double.class, long.class});
            fail();
        } catch (RequirementFailure e) {
            // pass
        }

        try {
            ModelFarmUtils.requireTable("TABLENAME", t, new String[] {"A", "C", "B"},
                    new Class[] {int.class, double.class, double.class});
            fail();
        } catch (RequirementFailure e) {
            // pass
        }
    }

    @Test
    public void testArrayString() {
        final String[] target = {"A", "B", "C"};
        final String[] result = ModelFarmUtils.arrayString(new ObjectVectorDirect<>(target));
        assertArrayEquals(target, result);
        assertNull(ModelFarmUtils.arrayString(null));
    }

    @Test
    public void testArrayInstant() {
        final Instant[] target = {
                DateTimeUtils.parseInstant("2018-01-11T01:01:01 NY"),
                DateTimeUtils.parseInstant("2018-02-11T01:01:01 NY"),
                DateTimeUtils.parseInstant("2018-03-11T01:01:01 NY")};
        final Instant[] result = ModelFarmUtils.arrayInstant(new ObjectVectorDirect<>(target));
        assertArrayEquals(target, result);
        assertNull(ModelFarmUtils.arrayInstant(null));
    }

    @Test
    public void testArrayFloat() {
        final float[] target = {1.1f, 2.2f, 3.3f};
        final float[] result = ModelFarmUtils.arrayFloat(new FloatVectorDirect(target));
        assertArrayEquals(target, result, 1e-10f);
        assertNull(ModelFarmUtils.arrayFloat(null));
    }

    @Test
    public void testArrayDouble() {
        final double[] target = {1.1, 2.2, 3.3};
        final double[] result = ModelFarmUtils.arrayDouble(new DoubleVectorDirect(target));
        assertArrayEquals(target, result, 1e-10);
        assertNull(ModelFarmUtils.arrayDouble(null));
    }

    @Test
    public void testArrayInt() {
        final int[] target = {1, 2, 3};
        final int[] result = ModelFarmUtils.arrayInt(new IntVectorDirect(target));
        assertArrayEquals(target, result);
        assertNull(ModelFarmUtils.arrayInt(null));
    }

    @Test
    public void testArrayLong() {
        final long[] target = {1, 2, 3};
        final long[] result = ModelFarmUtils.arrayLong(new LongVectorDirect(target));
        assertArrayEquals(target, result);
        assertNull(ModelFarmUtils.arrayLong(null));
    }

    @Test
    public void testArray2Double() {
        final double[][] target = {{1.1, 2.2, 3.3}, {5, 6}};
        final ObjectVector dba = new ObjectVectorDirect(
                new DoubleVectorDirect(target[0]),
                new DoubleVectorDirect(target[1]));
        final double[][] result = ModelFarmUtils.array2Double(dba);
        Asserts.assertEquals(target, result);
        assertNull(ModelFarmUtils.array2Double(null));
    }

}
