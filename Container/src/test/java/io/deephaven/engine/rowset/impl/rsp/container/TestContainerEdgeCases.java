//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.List;

import static io.deephaven.engine.rowset.impl.rsp.container.ContainerTestCommon.valuesOf;
import static org.junit.Assert.assertEquals;

/**
 * Edge cases of the container API that the {@code RspBitmap} layer never asks for (empty ranges, empty rank ranges, a
 * container as its own operand), but that every implementation must still answer the same way.
 */
public class TestContainerEdgeCases {

    /** Galloping must be chosen by how many values each side holds, not by how much room its array has. */
    @Test
    public void testIntersectionWithAnEmptyButRoomyArray() {
        final ArrayContainer one = new ArrayContainer(new short[] {0});
        final ArrayContainer roomy = new ArrayContainer(4000);
        assertEquals(List.of(), valuesOf(new ArrayContainer(new short[] {0}).and(roomy)));
        assertEquals(List.of(), valuesOf(one.iand(roomy)));
        assertEquals(List.of(), valuesOf(roomy.iand(new ArrayContainer(new short[] {0}))));

        // Stale values beyond the logical length must not be matched.
        final short[] stale = new short[200];
        stale[0] = 7;
        stale[1] = 9;
        final short[] buffer = new short[2];
        assertEquals(0, ContainerUtil.unsignedIntersect2by2(new short[] {7, 9}, 2, stale, 0, buffer));
        assertEquals(0, ContainerUtil.unsignedIntersect2by2(stale, 0, new short[] {7, 9}, 2, buffer));
    }
}
