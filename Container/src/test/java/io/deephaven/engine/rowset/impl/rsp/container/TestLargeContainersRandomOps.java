package io.deephaven.engine.rowset.impl.rsp.container;

import org.junit.Test;

import java.util.Random;
import java.util.function.BiFunction;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static io.deephaven.engine.rowset.impl.rsp.container.Container.MAX_RANGE;

public class TestLargeContainersRandomOps {
    public static final int numRuns = 5000;
    public static final int lastRun = 0;
    public static final int seed0 = 15 + lastRun;

    public static final float pEmpty = 0.01F;
    public static final float pElem = (float) (1.0 / 3.0);

    private static int randValueInRange(final Random rand, final int first, final int last) {
        return first + rand.nextInt(last - first + 1);
    }

    private static Container randomLargeContainer(
            final Random rand, final boolean asBitmapContainer, final int min, final int max) {
        return randomLargeContainer(rand, true, asBitmapContainer, min, max);
    }

    private static Container randomLargeContainer(
            final Random rand, final boolean allowEmpty, final boolean asBitmapContainer, final int min,
            final int max) {
        if (max - min < 2) {
            throw new IllegalArgumentException("min=" + min + ", max=" + max);
        }
        Container c;
        if (asBitmapContainer) {
            c = new BitmapContainer();
        } else if (rand.nextBoolean()) {
            c = new ArrayContainer();
        } else {
            c = new RunContainer();
        }
        if (allowEmpty && rand.nextFloat() < pEmpty) {
            return c;
        }
        int count = 0;
        for (int i = min; i <= max; ++i) {
            if (rand.nextFloat() < pElem) {
                ++count;
                c = c.iset((short) i);
            }
        }
        if (!allowEmpty && count == 0) {
            c = c.iset((short) randValueInRange(rand, min, max));
        }
        return c;
    }

    private static void testOpLoop(
            final Random rand, final BiFunction<Container, Container, Container> op, final boolean inPlace) {
        for (int i = 0; i < numRuns; ++i) {
            testOp(i, rand, op, inPlace);
        }
    }

    private static final int tdelta = 40;

    private static void testOp(final int i, final Random rand,
            final BiFunction<Container, Container, Container> op, final boolean inPlace) {
        testOp(i, rand, op, inPlace, 0, tdelta);
        testOp(i, rand, op, inPlace, 65535 - tdelta, 65535);
    }

    private static void testOp(
            final int i, final Random rand,
            final BiFunction<Container, Container, Container> op, final boolean inPlace,
            final int min, final int max) {
        final String m = "i==" + i + " && min==" + min + " && max==" + max;
        final Container c1 = randomLargeContainer(rand, false, min, max);
        final boolean asBitmapContainer = rand.nextBoolean();
        final Container c2 = randomLargeContainer(rand, asBitmapContainer, min, max);
        final Container c1Bitmap = c1.toBitmapContainer();
        final Container c2Bitmap = asBitmapContainer ? c2 : c2.toBitmapContainer();

        Container result = (inPlace ? op.apply(c1.deepCopy(), c2) : op.apply(c1, c2)).check();
        assertTrue(m, result != c2 || result.isShared());
        if (!inPlace) {
            assertTrue(m, result != c1 || result.isShared()); // none of the operations are inplace.
        }
        Container expected = (inPlace ? op.apply(c1Bitmap.deepCopy(), c2Bitmap) : op.apply(c1Bitmap, c2Bitmap)).check();
        assertTrue(m, expected.sameContents(result));
        assertTrue(m, expected != c2Bitmap || expected.isShared());
        if (!inPlace) {
            assertTrue(m, expected != c1Bitmap || expected.isShared()); // none of the operations are inplace.
        }

        result = (inPlace ? op.apply(c2.deepCopy(), c1) : op.apply(c2, c1)).check();
        assertTrue(m, result != c1 || result.isShared());
        if (!inPlace) {
            assertTrue(m, result != c2 || result.isShared()); // none of the operations are inplace.
        }
        expected = (inPlace ? op.apply(c2Bitmap.deepCopy(), c1Bitmap) : op.apply(c2Bitmap, c1Bitmap)).check();
        assertTrue(m, expected != c1Bitmap || expected.isShared());
        if (!inPlace) {
            assertTrue(m, expected != c2Bitmap || expected.isShared()); // none of the operations are inplace.
        }
        assertTrue(m, expected.sameContents(result));

        if (!inPlace) {
            return;
        }
        final Container c1CowRefUnused = c1.cowRef();
        final Container c1Copy = c1.deepCopy();
        result = op.apply(c1, c2);
        assertTrue(m, c1.sameContents(c1Copy));
        if (!result.sameContents(c1Copy) && !result.sameContents(c2)) {
            assertFalse(m, result.isShared());
        }
    }

    @Test
    public void testAnd() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::and, false);
    }

    @Test
    public void testOr() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::or, false);
    }

    @Test
    public void testXor() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::xor, false);
    }

    @Test
    public void testAndNot() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::andNot, false);
    }

    @Test
    public void testIAnd() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::iand, true);
    }

    @Test
    public void testIOr() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::ior, true);
    }

    @Test
    public void testIXor() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::ixor, true);
    }

    @Test
    public void testIAndNot() {
        final Random rand = new Random(seed0);
        testOpLoop(rand, Container::iandNot, true);
    }

    interface BooleanOps {
        boolean resultOp(Container c1, Container c2);

        boolean expectedOp(Container c1, Container c2);
    }

    private static void testBooleanOpLoop(final Random rand, final BooleanOps ops) {
        for (int i = 0; i < numRuns; ++i) {
            testBooleanOp(i, rand, ops);
        }
    }

    private static void testBooleanOp(final int i, final Random rand, final BooleanOps ops) {
        testBooleanOp(i, rand, ops, 0, tdelta);
        testBooleanOp(i, rand, ops, 65535 - tdelta, 65535);
    }

    private static void testBooleanOp(
            final int i, final Random rand, final BooleanOps ops, final int min, final int max) {
        final String m = "i==" + i + " && min==" + min + " && max==" + max;
        final Container c1 = randomLargeContainer(rand, false, min, max);
        final boolean asLargeContainer = rand.nextBoolean();
        final Container c2 = randomLargeContainer(rand, asLargeContainer, min, max);
        boolean result = ops.resultOp(c1, c2);
        boolean expected = ops.expectedOp(c1, c2);
        assertEquals(m, expected, result);
        result = ops.resultOp(c2, c1);
        expected = ops.expectedOp(c2, c1);
        assertEquals(m, expected, result);
    }

    @Test
    public void testOverlaps() {
        final Random rand = new Random(seed0);
        testBooleanOpLoop(rand, new BooleanOps() {
            @Override
            public boolean resultOp(final Container c1, final Container c2) {
                return c1.overlaps(c2);
            }

            @Override
            public boolean expectedOp(final Container c1, final Container c2) {
                return c1.toBitmapContainer().overlaps(c2.toBitmapContainer());
            }
        });
    }

    @Test
    public void testContains() {
        final Random rand = new Random(seed0);
        testBooleanOpLoop(rand, new BooleanOps() {
            @Override
            public boolean resultOp(final Container c1, final Container c2) {
                return c1.contains(c2);
            }

            @Override
            public boolean expectedOp(final Container c1, final Container c2) {
                return c1.toBitmapContainer().contains(c2.toBitmapContainer());
            }
        });
    }

    @Test
    public void testSubsetOf() {
        final Random rand = new Random(seed0);
        testBooleanOpLoop(rand, new BooleanOps() {
            @Override
            public boolean resultOp(final Container c1, final Container c2) {
                return c1.subsetOf(c2);
            }

            @Override
            public boolean expectedOp(final Container c1, final Container c2) {
                return c1.toBitmapContainer().subsetOf(c2.toBitmapContainer());
            }
        });
    }

    interface RangeOps {
        Container resultOp(Container c, int start, int end);

        Container expectedOp(Container c, int start, int end);
    }

    @Test
    public void testAddRange() {
        testRangeOp(new RangeOps() {
            @Override
            public Container resultOp(final Container c, final int start, final int end) {
                return c.add(start, end);
            }

            @Override
            public Container expectedOp(final Container c, final int start, final int end) {
                return c.toBitmapContainer().add(start, end);
            }
        });
    }

    @Test
    public void testRemoveRange() {
        testRangeOp(new RangeOps() {
            @Override
            public Container resultOp(final Container c, final int start, final int end) {
                return c.remove(start, end);
            }

            @Override
            public Container expectedOp(final Container c, final int start, final int end) {
                return c.toBitmapContainer().remove(start, end);
            }
        });
    }

    @Test
    public void testAndRange() {
        testRangeOp(new RangeOps() {
            @Override
            public Container resultOp(final Container c, final int start, final int end) {
                return c.andRange(start, end);
            }

            @Override
            public Container expectedOp(final Container c, final int start, final int end) {
                return c.toBitmapContainer().and(new RunContainer(start, end));
            }
        });
    }

    @Test
    public void testNotRange() {
        testRangeOp(new RangeOps() {
            @Override
            public Container resultOp(final Container c, final int start, final int end) {
                return c.not(start, end);
            }

            @Override
            public Container expectedOp(final Container c, final int start, final int end) {
                return c.toBitmapContainer().not(start, end);
            }
        });
    }

    private static void testRangeOp(final RangeOps ops) {
        final Random rand = new Random(seed0);
        final int[][] minmaxes = new int[][] {new int[] {0, tdelta}, new int[] {65535 - tdelta, 65535}};
        for (int run = 0; run < numRuns; ++run) {
            final String m = "run==" + run;
            for (int[] minmax : minmaxes) {
                final String m2 = m + " && minmax[0]==" + minmax[0] + " && minmax[1]==" + minmax[1];
                final Container c = randomLargeContainer(rand, false, minmax[0], minmax[1]);
                for (int i = 0; i < tdelta; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final int rmin = minmax[0] == 0 ? 0 : rand.nextInt(minmax[0] - tdelta);
                    final int rmax = minmax[1] == 65535 ? 65535 : rand.nextInt(minmax[1] + tdelta);
                    final int first = randValueInRange(rand, rmin, rmax);
                    final int last = randValueInRange(rand, first, rmax);
                    final Container r = ops.resultOp(c, first, last + 1).check();
                    final Container expected = ops.expectedOp(c, first, last + 1).check();
                    assertEquals(m3, expected.getCardinality(), r.getCardinality());
                    assertTrue(m3, expected.subsetOf(r));
                }
            }
        }
    }

    interface RangeBooleanOps {
        boolean resultOp(Container c, int start, int end);

        boolean expectedOp(Container c, int start, int end);
    }

    @Test
    public void testOverlapsRange() {
        testRangeBooleanOp(new RangeBooleanOps() {
            @Override
            public boolean resultOp(final Container c, final int start, final int end) {
                return c.overlapsRange(start, end);
            }

            @Override
            public boolean expectedOp(final Container c, final int start, final int end) {
                return c.toBitmapContainer().overlapsRange(start, end);
            }
        });
    }

    @Test
    public void testContainsRange() {
        testRangeBooleanOp(new RangeBooleanOps() {
            @Override
            public boolean resultOp(final Container c, final int start, final int end) {
                return c.contains(start, end);
            }

            @Override
            public boolean expectedOp(final Container c, final int start, final int end) {
                return c.toBitmapContainer().contains(start, end);
            }
        });
    }

    private static void testRangeBooleanOp(final RangeBooleanOps ops) {
        final Random rand = new Random(seed0);
        final int[][] minmaxes = new int[][] {new int[] {0, tdelta}, new int[] {65535 - tdelta, 65535}};
        for (int run = 0; run < numRuns; ++run) {
            final String m = "run==" + run;
            for (int[] minmax : minmaxes) {
                final String m2 = m + " && minmax[0]==" + minmax[0] + " && minmax[1]==" + minmax[1];
                final Container c = randomLargeContainer(rand, false, minmax[0], minmax[1]);
                for (int i = 0; i < tdelta; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final int rmin = minmax[0] == 0 ? 0 : rand.nextInt(minmax[0] - tdelta);
                    final int rmax = minmax[1] == 65535 ? 65535 : rand.nextInt(minmax[1] + tdelta);
                    final int first = randValueInRange(rand, rmin, rmax);
                    final int last = randValueInRange(rand, first, rmax);
                    final boolean result = ops.resultOp(c, first, last + 1);
                    final boolean expected = ops.expectedOp(c, first, last + 1);
                    assertEquals(m3, expected, result);
                }
            }
        }
    }

    interface ValueOps {
        Container resultOp(Container c, int v);

        Container expectedOp(Container c, int v);
    }

    @Test
    public void testSet() {
        testValueOp(new ValueOps() {
            @Override
            public Container resultOp(final Container c, final int v) {
                return c.set((short) v);
            }

            @Override
            public Container expectedOp(final Container c, final int v) {
                return c.toBitmapContainer().set((short) v);
            }
        });
    }

    @Test
    public void testUnset() {
        testValueOp(new ValueOps() {
            @Override
            public Container resultOp(final Container c, final int v) {
                return c.unset((short) v);
            }

            @Override
            public Container expectedOp(final Container c, final int v) {
                return c.toBitmapContainer().unset((short) v);
            }
        });
    }

    @Test
    public void testFlip() {
        testValueOp(new ValueOps() {
            @Override
            public Container resultOp(final Container c, final int v) {
                return c.cowRef().iflip((short) v);
            }

            @Override
            public Container expectedOp(final Container c, final int v) {
                return c.cowRef().toBitmapContainer().iflip((short) v);
            }
        });
    }

    private static void testValueOp(final ValueOps ops) {
        final Random rand = new Random(seed0);
        final int[][] minmaxes = new int[][] {new int[] {0, tdelta}, new int[] {65535 - tdelta, 65535}};
        for (int run = 0; run < numRuns; ++run) {
            final String m = "run==" + run;
            for (int[] minmax : minmaxes) {
                final String m2 = m + " && minmax[0]==" + minmax[0] + " && minmax[1]==" + minmax[1];
                final Container c = randomLargeContainer(rand, false, minmax[0], minmax[1]);
                for (int i = 0; i < 10; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final int rmin = minmax[0] == 0 ? 0 : rand.nextInt(minmax[0] - tdelta);
                    final int rmax = minmax[1] == 65535 ? 65535 : rand.nextInt(minmax[1] + tdelta);
                    final int value = randValueInRange(rand, rmin, rmax);
                    final Container r = ops.resultOp(c, value).check();
                    final Container expected = ops.expectedOp(c, value).check();
                    assertEquals(m3, expected.getCardinality(), r.getCardinality());
                    assertTrue(m3, expected.subsetOf(r));
                }
            }
        }
    }

    interface ValueBooleanOps {
        boolean resultOp(Container c, int v);

        boolean expectedOp(Container c, int v);
    }

    @Test
    public void testContainsValue() {
        testValueBooleanOp(new ValueBooleanOps() {
            @Override
            public boolean resultOp(final Container c, final int v) {
                return c.contains((short) v);
            }

            @Override
            public boolean expectedOp(final Container c, final int v) {
                return c.toBitmapContainer().contains((short) v);
            }
        });
    }

    private static void testValueBooleanOp(final ValueBooleanOps ops) {
        final Random rand = new Random(seed0);
        final int[][] minmaxes = new int[][] {new int[] {0, tdelta}, new int[] {65535 - tdelta, 65535}};
        for (int run = 0; run < numRuns; ++run) {
            final String m = "run==" + run;
            for (int[] minmax : minmaxes) {
                final String m2 = m + " && minmax[0]==" + minmax[0] + " && minmax[1]==" + minmax[1];
                final Container c = randomLargeContainer(rand, false, minmax[0], minmax[1]);
                for (int i = 0; i < tdelta; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final int rmin = minmax[0] == 0 ? 0 : rand.nextInt(minmax[0] - tdelta);
                    final int rmax = minmax[1] == 65535 ? 65535 : rand.nextInt(minmax[1] + tdelta);
                    final int value = randValueInRange(rand, rmin, rmax);
                    final boolean result = ops.resultOp(c, value);
                    final boolean expected = ops.expectedOp(c, value);
                    assertEquals(m3, expected, result);
                }
            }
        }
    }

    @Test
    public void testAppend() {
        final Random rand = new Random(seed0);
        final int[][] minmaxes = new int[][] {new int[] {0, tdelta}, new int[] {65535 - tdelta, 65534}};
        for (int run = 0; run < numRuns; ++run) {
            final String m = "run==" + run;
            for (int[] minmax : minmaxes) {
                final String m2 = m + " && minmax[0]==" + minmax[0] + " && minmax[1]==" + minmax[1];
                final Container c = randomLargeContainer(rand, false, minmax[0], minmax[1]);
                final int trials = c.isEmpty() ? tdelta : Math.min(65535 - c.last(), tdelta);
                for (int i = 0; i < trials; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final int rmin = c.isEmpty() ? 0 : c.last() + 1;
                    final int rmax = 65535;
                    final int first = randValueInRange(rand, rmin, rmax);
                    final int last = randValueInRange(rand, first, rmax);
                    final Container result = c.cowRef().iappend(first, last + 1).check();
                    final Container expected = c.cowRef().toBitmapContainer().iappend(first, last + 1).check();
                    assertEquals(m3, expected.getCardinality(), result.getCardinality());
                    assertTrue(m3, expected.subsetOf(result));
                }
            }
        }
    }

    interface ValueIntOps {
        int pick(Random r, Container c);

        int resultOp(Container c, int v);

        int expectedOp(Container c, int v);
    }

    @Test
    public void testSelect() {
        testValueIntOp(new ValueIntOps() {
            @Override
            public int pick(final Random r, final Container c) {
                return r.nextInt(c.getCardinality());
            }

            @Override
            public int resultOp(final Container c, final int v) {
                return c.select(v);
            }

            @Override
            public int expectedOp(final Container c, final int v) {
                return c.toBitmapContainer().select(v);
            }
        });
    }

    @Test
    public void testFind() {
        testValueIntOp(new ValueIntOps() {
            @Override
            public int pick(final Random r, final Container c) {
                return c.select((short) r.nextInt(c.getCardinality()));
            }

            @Override
            public int resultOp(final Container c, final int v) {
                return c.find((short) v);
            }

            @Override
            public int expectedOp(final Container c, final int v) {
                return c.toBitmapContainer().find((short) v);
            }
        });
    }

    @Test
    public void testRank() {
        testValueIntOp(new ValueIntOps() {
            @Override
            public int pick(final Random r, final Container c) {
                final int card = c.getCardinality();
                final int bound = Math.min(card + 1, MAX_RANGE);
                return r.nextInt(bound);
            }

            @Override
            public int resultOp(final Container c, final int v) {
                return c.rank((short) v);
            }

            @Override
            public int expectedOp(final Container c, final int v) {
                return c.toBitmapContainer().rank((short) v);
            }
        });
    }

    private static void testValueIntOp(final ValueIntOps ops) {
        final Random rand = new Random(seed0);
        final int[][] minmaxes = new int[][] {new int[] {0, tdelta}, new int[] {65535 - tdelta, 65535}};
        for (int run = 0; run < numRuns; ++run) {
            final String m = "run==" + run;
            for (int[] minmax : minmaxes) {
                final String m2 = m + " && minmax[0]==" + minmax[0] + " && minmax[1]==" + minmax[1];
                final Container c = randomLargeContainer(rand, false, false, minmax[0], minmax[1]);
                for (int i = 0; i < tdelta; ++i) {
                    final String m3 = m2 + " && i==" + i;
                    final int value = ops.pick(rand, c);
                    final int result = ops.resultOp(c, value);
                    final int expected = ops.expectedOp(c, value);
                    assertEquals(m3, expected, result);
                }
            }
        }
    }
}
