//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.rsp.container.ContainerTestCommon.valuesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Edge cases of the container API that the {@code RspBitmap} layer never asks for (empty ranges, empty rank ranges, a
 * container as its own operand), but that every implementation must still answer the same way.
 */
public class TestContainerEdgeCases {

    private static final int[] RUNS = {1, 5, 10, 20, 30, 31, 40, 50};

    /** One fresh fixture per implementation; each one is built anew so in-place operations cannot leak across. */
    private static List<Supplier<Container>> fixtures() {
        final List<Supplier<Container>> out = new ArrayList<>();
        out.add(() -> {
            final ArrayContainer c = new ArrayContainer(64);
            for (int ii = 0; ii < RUNS.length; ii += 2) {
                for (int v = RUNS[ii]; v < RUNS[ii + 1]; ++v) {
                    c.iset((short) v);
                }
            }
            return c;
        });
        out.add(() -> {
            final BitmapContainer c = new BitmapContainer();
            for (int ii = 0; ii < RUNS.length; ii += 2) {
                c.iadd(RUNS[ii], RUNS[ii + 1]);
            }
            return c;
        });
        out.add(() -> {
            Container c = new RunContainer(RUNS[0], RUNS[1], RUNS[2], RUNS[3]);
            for (int ii = 4; ii < RUNS.length; ii += 2) {
                c = c.iadd(RUNS[ii], RUNS[ii + 1]);
            }
            return c;
        });
        out.add(() -> Container.twoValues((short) 10, (short) 20));
        out.add(() -> Container.singleton((short) 10));
        out.add(() -> new SingleRangeContainer(10, 20));
        out.add(Container::empty);
        return out;
    }

    private static String name(final Container c) {
        return c.getClass().getSimpleName();
    }

    /** A {@code false} from advance leaves the iterator exhausted, whatever it had returned before. */
    @Test
    public void testReverseAdvanceFailureExhausts() {
        for (final Supplier<Container> fixture : fixtures()) {
            final List<Integer> descending = valuesOf(fixture.get());
            Collections.reverse(descending);
            for (int consumed = 1; consumed <= descending.size(); ++consumed) {
                for (int v = 0; v <= 52; ++v) {
                    final Container c = fixture.get();
                    final ShortAdvanceIterator it = c.getReverseShortIterator();
                    for (int ii = 0; ii < consumed; ++ii) {
                        it.next();
                    }
                    final String m = name(c) + " after " + consumed + " advance(" + v + ")";
                    int expectedAt = -1;
                    for (int ii = consumed - 1; ii < descending.size(); ++ii) {
                        if (descending.get(ii) <= v) {
                            expectedAt = ii;
                            break;
                        }
                    }
                    final boolean found = it.advance(v);
                    assertEquals(m, expectedAt != -1, found);
                    if (found) {
                        assertEquals(m + " curr", (int) descending.get(expectedAt), it.currAsInt());
                        assertEquals(m + " hasNext", expectedAt + 1 < descending.size(), it.hasNext());
                    } else {
                        assertFalse(m + " hasNext", it.hasNext());
                    }
                }
            }
        }
    }

    private static void checkSelfOperand(final String op, final BinaryOperator<Container> f,
            final boolean keepsValues) {
        for (final Supplier<Container> fixture : fixtures()) {
            final Container c = fixture.get();
            final List<Integer> expected = keepsValues ? valuesOf(c) : List.of();
            final String m = name(c) + "." + op + "(itself)";
            final Container result = f.apply(c, c);
            result.validate();
            assertEquals(m, expected, valuesOf(result));
            assertEquals(m + " cardinality", expected.size(), result.getCardinality());
        }
    }

    @Test
    public void testSelfOperand() {
        checkSelfOperand("or", Container::or, true);
        checkSelfOperand("and", Container::and, true);
        checkSelfOperand("andNot", Container::andNot, false);
        checkSelfOperand("xor", Container::xor, false);
        checkSelfOperand("ior", Container::ior, true);
        checkSelfOperand("iand", Container::iand, true);
        checkSelfOperand("iandNot", Container::iandNot, false);
        checkSelfOperand("ixor", Container::ixor, false);
    }

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
