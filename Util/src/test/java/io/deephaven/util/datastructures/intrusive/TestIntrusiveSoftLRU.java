/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.datastructures.intrusive;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.*;

import static org.junit.Assert.*;

public class TestIntrusiveSoftLRU {

    private final IntrusiveSoftLRU.Adapter<TestNode> ADAPTER = new IntrusiveSoftLRU.Node.Adapter<>();

    static class TestNode extends IntrusiveSoftLRU.Node.Impl<TestNode> {
        final int index;

        TestNode(int index) {
            this.index = index;
        }
    }

    @Test
    public void testBasics() {
        final List<WeakReference<TestNode>> objs = new LinkedList<>();
        final IntrusiveSoftLRU<TestNode> lru = new IntrusiveSoftLRU<>(ADAPTER, 10, 10);

        for (int i = 0; i < 20; ++i) {
            final TestNode testNode = new TestNode(i);
            objs.add(new WeakReference<>(testNode));
            lru.touch(testNode);
        }

        System.gc();
        iterateAssertAndCount(objs, lru);

        for (final ListIterator<WeakReference<TestNode>> i = objs.listIterator(objs.size()); i.hasPrevious();) {
            final TestNode testNode = i.previous().get();

            if (testNode != null) {
                lru.touch(testNode);
            }
        }

        iterateAssertAndCount(objs, lru);
        lru.clear();
        System.gc();

        for (final WeakReference<TestNode> objRef : objs) {
            assertNull(objRef.get());
        }
    }

    @Test
    public void testRobustness() {
        final List<TestNode> objects = new ArrayList<>(1000);
        final List<WeakReference<TestNode>> objRefs = new ArrayList<>(1000);
        final IntrusiveSoftLRU<TestNode> lru = new IntrusiveSoftLRU<>(ADAPTER, 100, 100);

        for (int i = 0; i < 1000; ++i) {
            createAndAddToCache(objects, objRefs, lru, i);
        }

        assertTrue(lru.verifyLRU(100));

        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 10; ++j) {
                lru.touch(objects.get(j));
            }

            assertTrue(lru.verifyLRU(100));

            Collections.shuffle(objects);
            objects.subList(0, 10).clear();
            System.gc();
        }

        objRefs.removeIf(x -> x.get() == null);
        assertEquals(objRefs.size(), 100);
    }

    @Test
    public void testExpandToMax() {
        final List<WeakReference<TestNode>> objRefs = new ArrayList<>(512);
        final IntrusiveSoftLRU<TestNode> lru = new IntrusiveSoftLRU<>(ADAPTER, 16, 256);

        // Insert stuff and verify that we grow.
        for (int i = 0; i < 512; ++i) {
            createAndAddToCache(null, objRefs, lru, i);

            // Check the we resize by doubling and don't internally shuffle things before we are LRU
            if (i == 15 || i == 31 || i == 63 || i == 127) {
                assertEquals(i + 1, lru.currentCapacity());
                System.gc();
                for (int jj = 0; jj <= i; jj++) {
                    final TestNode object = objRefs.get(jj).get();
                    assertNotNull(object);
                    assertEquals(jj, object.getSlot());
                    lru.touch(object);
                    assertEquals(jj, object.getSlot());
                    assertEquals(i + 1, lru.currentCapacity());
                }
            }
        }

        System.gc();
        iterateAssertAndCount(objRefs, lru);
        assertTrue(lru.verifyLRU(256));

        for (int ii = 0; ii < 16; ii++) {
            createAndAddToCache(null, objRefs, lru, ii);
        }

        System.gc();
        assertTrue(lru.verifyLRU(256));

        final Set<Integer> used = new HashSet<>();
        for (final Iterator<WeakReference<TestNode>> i = objRefs.iterator(); i.hasNext();) {
            final TestNode testNode = i.next().get();
            if (testNode == null) {
                // We should have bumped the middle 16 items of the original collection from the LRU
                i.remove();
            } else {
                assertTrue(testNode.index < 16 || testNode.index >= 272);
                assertEquals(testNode, ADAPTER.getOwner(testNode).get());
                assertTrue(used.add(ADAPTER.getSlot(testNode)));
            }
        }
    }

    @Test
    public void testResizeWithCompact() {
        final List<WeakReference<TestNode>> objs = new LinkedList<>();
        final IntrusiveSoftLRU<TestNode> lru = new IntrusiveSoftLRU<>(ADAPTER, 10, 40);

        for (int i = 0; i < 10; ++i) {
            createAndAddToCache(null, objs, lru, i);
        }

        // Now we'll artificially null out some references
        for (int refIdx = 0; refIdx < 10; refIdx += 2) {
            lru.evict(refIdx);
        }

        createAndAddToCache(null, objs, lru, 10);
        assertEquals(10, lru.currentCapacity());
        assertEquals(6, lru.size());
    }

    private void iterateAssertAndCount(@NotNull final List<WeakReference<TestNode>> objs,
            @NotNull final IntrusiveSoftLRU<TestNode> lru) {
        int nonNullItems = 0;
        final Set<Integer> used = new HashSet<>();
        for (final Iterator<WeakReference<TestNode>> i = objs.iterator(); i.hasNext();) {
            final TestNode testNode = i.next().get();

            if (testNode == null) {
                i.remove();
            } else {
                nonNullItems++;
                assertTrue(testNode.index >= lru.currentCapacity());
                assertEquals(testNode, ADAPTER.getOwner(testNode).get());
                assertTrue(used.add(ADAPTER.getSlot(testNode)));
            }
        }

        assertEquals(lru.currentCapacity(), nonNullItems);
    }

    private void createAndAddToCache(@Nullable final List<TestNode> objects,
            @NotNull final List<WeakReference<TestNode>> objRefs,
            @NotNull final IntrusiveSoftLRU<TestNode> lru, int i) {
        final TestNode testNode = new TestNode(i);
        if (objects != null) {
            objects.add(testNode);
        }
        objRefs.add(new WeakReference<>(testNode));
        lru.touch(testNode);
    }
}
