package io.deephaven.integrations.learn;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public class IndexSetTest {

    static IndexSet indexSet;

    @Test
    public void AddVerifyRemoveTest() throws Exception {
        indexSet = new IndexSet(5);

        TestCase.assertEquals(-1, indexSet.current);
        TestCase.assertEquals(5, indexSet.maxSize);

        indexSet.add(135);
        indexSet.add(54);
        indexSet.add(100000);

        PrimitiveIterator.OfLong itr = indexSet.iterator();
        TestCase.assertEquals(3, indexSet.size());
        TestCase.assertEquals(135, (long) itr.next());
        TestCase.assertEquals(54, (long) itr.next());
        TestCase.assertEquals(100000, (long) itr.next());

        indexSet.clear();

        TestCase.assertEquals(-1, indexSet.current);
        TestCase.assertEquals(5, indexSet.maxSize);
    }

    @Test(expected = Exception.class)
    public void TooManyElementsTest() throws Exception {
        indexSet = new IndexSet(5);

        for (int i = 0 ; i < 6 ; i++) {
            indexSet.add(i);
        }
        indexSet.clear();
    }

    @Test(expected = NoSuchElementException.class)
    public void ParseTooFarTest() throws Exception {
        indexSet = new IndexSet(5);

        indexSet.add(135);
        indexSet.add(54);
        indexSet.add(100000);

        PrimitiveIterator.OfLong itr = indexSet.iterator();
        for (int i = 0 ; i < 5 ; i++) {
            itr.next();
        }
        indexSet.clear();
    }

    @Test
    public void RightSizeTest() throws Exception {
        indexSet = new IndexSet(5);

        indexSet.add(135);
        indexSet.add(54);
        indexSet.add(100000);
        TestCase.assertEquals(false, indexSet.isFull());

        indexSet.add(42);
        indexSet.add(600);
        TestCase.assertEquals(true, indexSet.isFull());

        indexSet.clear();
    }

    @Test(expected = IllegalArgumentException.class)
    public void PositiveMaxSizeTest() {
        indexSet = new IndexSet(0);
    }
}