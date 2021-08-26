/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.cache;

import io.deephaven.base.Procedure;
import io.deephaven.hash.KeyedObjectKey;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Random;

@SuppressWarnings({"RedundantStringConstructorCall", "JUnit4AnnotatedMethodInJUnit3TestCase"})
public class TestKeyedObjectCache extends TestCase {

    private static final KeyedObjectKey<Object, Object> KEY =
        new KeyedObjectKey.Basic<Object, Object>() {
            @Override
            public Object getKey(Object o) {
                return o;
            }
        };
    private static final Procedure.Unary<Object> POST = new Procedure.Unary<Object>() {
        @Override
        public void call(Object arg) {
            lastPost = arg;
        }
    };
    private static final Random RANDOM = new Random() {
        @Override
        public int nextInt(int n) {
            return nextInt;
        }
    };

    private static Object lastPost = null;
    private static int nextInt = 0;

    @Test
    public void testKeyedObjectCache() {
        final KeyedObjectCache<Object, Object> cache =
            new KeyedObjectCache<>(6, 2, KEY, POST, RANDOM);
        TestCase.assertEquals(7, cache.getCapacity());
        TestCase.assertEquals(2, cache.getProbeSequenceLength());

        // A=65, hashes to 65, first bucket is 65 % 7 == 2, second bucket is 2 - (1 + (65 % (7 -
        // 2))) == 1
        // B=66, hashes to 66, first bucket is 66 % 7 == 3, second bucket is 3 - (1 + (66 % (7 -
        // 2))) == 1
        // C=67, hashes to 67, first bucket is 67 % 7 == 4, second bucket is 4 - (1 + (67 % (7 -
        // 2))) == 1
        // D=68, hashes to 68, first bucket is 68 % 7 == 5, second bucket is 5 - (1 + (68 % (7 -
        // 2))) == 1
        // E=69, hashes to 69, first bucket is 69 % 7 == 6, second bucket is 6 - (1 + (69 % (7 -
        // 2))) == 1
        // F=70, hashes to 70, first bucket is 70 % 7 == 0, second bucket is 0 - (1 + (70 % (7 -
        // 2))) + 7 == 6
        // G=71, hashes to 71, first bucket is 71 % 7 == 1, second bucket is 1 - (1 + (71 % (7 -
        // 2))) + 7 == 6
        // H=72, hashes to 72, first bucket is 72 % 7 == 2, second bucket is 2 - (1 + (72 % (7 -
        // 2))) + 7 == 6
        // I=73, hashes to 73, first bucket is 73 % 7 == 3, second bucket is 3 - (1 + (73 % (7 -
        // 2))) + 7 == 6

        // Fill bucket 2
        final String A = "A";
        TestCase.assertNull(cache.get(A));
        TestCase.assertSame(A, cache.putIfAbsent(A));
        TestCase.assertSame(A, cache.get(A));
        TestCase.assertSame(A, cache.get(new String("A")));
        TestCase.assertSame(A, cache.putIfAbsent(new String("A")));

        // 2 is filled, so fill bucket 6
        final String H = "H";
        TestCase.assertNull(cache.get(H));
        TestCase.assertSame(H, cache.putIfAbsent(H));
        TestCase.assertSame(H, cache.get(H));
        TestCase.assertSame(H, cache.get(new String("H")));
        TestCase.assertSame(H, cache.putIfAbsent(new String("H")));

        // 6 is filled, so fill bucket 1
        final String E = "E";
        TestCase.assertNull(cache.get(E));
        TestCase.assertSame(E, cache.putIfAbsent(E));
        TestCase.assertSame(E, cache.get(E));
        TestCase.assertSame(E, cache.get(new String("E")));
        TestCase.assertSame(E, cache.putIfAbsent(new String("E")));

        // 1 and 6 are filled, "randomly" select to evict E from 1
        nextInt = 0;
        final String G = "G";
        TestCase.assertNull(cache.get(G));
        TestCase.assertSame(G, cache.putIfAbsent(G));
        TestCase.assertSame(G, cache.get(G));
        TestCase.assertEquals(E, lastPost);
        TestCase.assertNull(cache.get(E));
        TestCase.assertSame(G, cache.get(new String("G")));
        TestCase.assertSame(G, cache.putIfAbsent(new String("G")));

        // 6 and 1 are filled, "randomly" select to evict G from 1
        nextInt = 1;
        TestCase.assertSame(E, cache.putIfAbsent(E));
        TestCase.assertSame(E, cache.get(E));
        TestCase.assertEquals(G, lastPost);
        TestCase.assertNull(cache.get(G));
        TestCase.assertSame(E, cache.get(new String("E")));
        TestCase.assertSame(E, cache.putIfAbsent(new String("E")));

        // 1 and 6 are filled, "randomly" select to evict H from 6
        nextInt = 1;
        TestCase.assertSame(G, cache.putIfAbsent(G));
        TestCase.assertSame(G, cache.get(G));
        TestCase.assertEquals(H, lastPost);
        TestCase.assertNull(cache.get(H));
        TestCase.assertSame(G, cache.get(new String("G")));
        TestCase.assertSame(G, cache.putIfAbsent(new String("G")));
    }
}
