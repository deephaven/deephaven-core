package io.deephaven.integrations.learn;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

public class IndexSetTest {

    @Test
    public void addVerifyRemoveTest() throws Exception {
        IndexSet indexSet = new IndexSet(5);

        TestCase.assertEquals(0, indexSet.getSize());
        indexSet.add(135);
        TestCase.assertEquals(1, indexSet.getSize());
        indexSet.add(54);
        TestCase.assertEquals(2, indexSet.getSize());
        indexSet.add(100000);
        TestCase.assertEquals(3, indexSet.getSize());

        PrimitiveIterator.OfLong itr = indexSet.iterator();
        TestCase.assertEquals(3, indexSet.getSize());
        TestCase.assertEquals(135, (long) itr.next());
        TestCase.assertEquals(54, (long) itr.next());
        TestCase.assertEquals(100000, (long) itr.next());

        PrimitiveIterator.OfLong itr2 = indexSet.iterator();
        TestCase.assertEquals(3, indexSet.getSize());
        TestCase.assertEquals(135, itr2.nextLong());
        TestCase.assertEquals(54, itr2.nextLong());
        TestCase.assertEquals(100000, itr2.nextLong());
    }

    @Test(expected = Exception.class)
    public void tooManyElementsTest() throws Exception {
        IndexSet indexSet = new IndexSet(5);

        for (int i = 0; i < 6; i++) {
            indexSet.add(i);
        }
    }

    @Test(expected = NoSuchElementException.class)
    public void parseTooFarTest() throws Exception {
        IndexSet indexSet = new IndexSet(5);

        indexSet.add(135);
        indexSet.add(54);
        indexSet.add(100000);

        PrimitiveIterator.OfLong itr = indexSet.iterator();
        for (int i = 0; i < 4; i++) {
            itr.next();
        }
    }

    @Test
    public void rightSizeTest() throws Exception {
        IndexSet indexSet = new IndexSet(5);

        indexSet.add(135);
        TestCase.assertEquals(false, indexSet.isFull());
        indexSet.add(54);
        TestCase.assertEquals(false, indexSet.isFull());
        indexSet.add(100000);
        TestCase.assertEquals(false, indexSet.isFull());
        indexSet.add(42);
        TestCase.assertEquals(false, indexSet.isFull());
        indexSet.add(600);
        TestCase.assertEquals(true, indexSet.isFull());
    }

    @Test(expected = IllegalArgumentException.class)
    public void positiveMaxSizeTest() {
        new IndexSet(0);
    }
}
