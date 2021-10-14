package io.deephaven.engine.util.datastructures.intrusive;

import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.datastructures.intrusive.IntrusiveSoftLRU;
import junit.framework.TestCase;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.*;
import org.junit.experimental.categories.Category;

@Category(OutOfBandTest.class)
public class TestIntrusiveSoftLRU {

    class Obj extends IntrusiveSoftLRU.Node.Impl<Obj> {
        final int index;

        Obj(int index) {
            this.index = index;
        }
    }

    @Test
    public void testBasics() {
        LinkedList<WeakReference<Obj>> objs = new LinkedList<>();

        IntrusiveSoftLRU.Adapter<Obj> adapter = new IntrusiveSoftLRU.Node.Adapter<>();
        IntrusiveSoftLRU<Obj> lru = new IntrusiveSoftLRU<>(adapter, 10);

        for (int i = 0; i < 20; ++i) {
            Obj obj = new Obj(i);

            objs.add(new WeakReference<>(obj));
            lru.touch(obj);
        }

        System.gc();

        Set<Integer> used = new HashSet<>();

        for (Iterator<WeakReference<Obj>> i = objs.iterator(); i.hasNext(); ) {
            Obj obj = i.next().get();

            if (obj == null) {
                i.remove();
            } else {
                TestCase.assertTrue(obj.index >= 10);
                TestCase.assertEquals(obj, adapter.getOwner(obj).get());
                TestCase.assertTrue(used.add(adapter.getSlot(obj)));
            }
        }

        for (ListIterator<WeakReference<Obj>> i = objs.listIterator(objs.size()); i.hasPrevious(); ) {
            Obj obj = i.previous().get();

            if (obj != null) {
                lru.touch(obj);
            }
        }

        used.clear();

        for (Iterator<WeakReference<Obj>> i = objs.iterator(); i.hasNext(); ) {
            Obj obj = i.next().get();

            if (obj == null) {
                i.remove();
            } else {
                TestCase.assertTrue(obj.index >= 10);
                TestCase.assertEquals(obj, adapter.getOwner(obj).get());
                TestCase.assertTrue(used.add(adapter.getSlot(obj)));
            }
        }

        lru.clear();
        System.gc();

        for (WeakReference<Obj> objRef : objs) {
            TestCase.assertNull(objRef.get());
        }
    }

    @Test
    public void testRobustness() {
        ArrayList<Obj> objects = new ArrayList<>(1000);
        ArrayList<WeakReference<Obj>> objRefs = new ArrayList<>(1000);

        IntrusiveSoftLRU.Adapter<Obj> adapter = new IntrusiveSoftLRU.Node.Adapter<>();
        IntrusiveSoftLRU<Obj> lru = new IntrusiveSoftLRU<>(adapter, 100);

        for (int i = 0; i < 1000; ++i) {
            Obj obj = new Obj(i);
            objects.add(obj);
            objRefs.add(new WeakReference<>(obj));
            lru.touch(obj);
        }

        TestCase.assertTrue(lru.verify(100));

        for (int i = 0; i < 100; ++i) {
            for (int j = 0; j < 10; ++j) {
                lru.touch(objects.get(j));
            }

            TestCase.assertTrue(lru.verify(100));

            Collections.shuffle(objects);
            objects.subList(0, 10).clear();
            System.gc();
        }

        objRefs.removeIf(x -> x.get() == null);
        TestCase.assertEquals(objRefs.size(), 100);
    }
}
