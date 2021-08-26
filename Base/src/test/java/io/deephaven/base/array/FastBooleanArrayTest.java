/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.array;

import junit.framework.TestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Random;

public class FastBooleanArrayTest extends TestCase {

    public void testAdd() {
        FastBooleanArray array = new FastBooleanArray();
        assertEquals(0, array.getLength());

        boolean item1 = true;
        array.add(item1);
        assertEquals(1, array.getLength());
        assertEquals(array.getUnsafeArray()[0], item1);

        boolean item2 = false;
        array.add(item2);
        assertEquals(2, array.getLength());
        assertEquals(array.getUnsafeArray()[1], item2);

        boolean item3 = true;
        array.add(item3);
        assertEquals(3, array.getLength());
        assertEquals(array.getUnsafeArray()[2], item3);

        boolean item4 = false;
        array.add(item4);
        assertEquals(4, array.getLength());
        assertEquals(array.getUnsafeArray()[3], item4);

        boolean item5 = true;
        array.add(item5);
        assertEquals(5, array.getLength());
        assertEquals(array.getUnsafeArray()[4], item5);

        array.quickReset();
        assertEquals(0, array.getLength());
    }

    public void testRemove() {
        FastBooleanArray array = new FastBooleanArray();
        assertEquals(0, array.getLength());

        // try to remove on an empty array
        try {
            array.removeThisIndex(0);
            fail("removing anything from an empty array should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(99);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

        // add a bunch to build up the array
        boolean item1 = true;
        array.add(item1);
        assertEquals(1, array.getLength());
        assertTrue(array.getUnsafeArray()[0] == item1);


        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(1);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }


        // add a bunch
        boolean item2 = false;
        array.add(item2);
        assertEquals(2, array.getLength());
        assertTrue(array.getUnsafeArray()[1] == item2);

        boolean item3 = true;
        array.add(item3);
        assertEquals(3, array.getLength());
        assertTrue(array.getUnsafeArray()[2] == item3);

        boolean item4 = false;
        array.add(item4);
        assertEquals(4, array.getLength());
        assertTrue(array.getUnsafeArray()[3] == item4);

        boolean item5 = true;
        array.add(item5);
        assertEquals(5, array.getLength());
        assertTrue(array.getUnsafeArray()[4] == item5);

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(5);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

        // remove
        array.removeThisIndex(0);
        assertEquals(4, array.getLength());
        assertTrue(array.getUnsafeArray()[0] == item2);
        assertTrue(array.getUnsafeArray()[1] == item3);
        assertTrue(array.getUnsafeArray()[2] == item4);
        assertTrue(array.getUnsafeArray()[3] == item5);

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(4);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

        // remove
        array.removeThisIndex(2);
        assertEquals(3, array.getLength());
        assertTrue(array.getUnsafeArray()[0] == item2);
        assertTrue(array.getUnsafeArray()[1] == item3);
        assertTrue(array.getUnsafeArray()[2] == item5);

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(3);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

        // remove
        array.removeThisIndex(1);
        assertEquals(2, array.getLength());
        assertTrue(array.getUnsafeArray()[0] == item2);
        assertTrue(array.getUnsafeArray()[1] == item5);

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(2);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

        // remove
        array.removeThisIndex(1);
        assertEquals(1, array.getLength());
        assertTrue(array.getUnsafeArray()[0] == item2);

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(1);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

        // remove to make empty
        array.removeThisIndex(0);
        assertEquals(0, array.getLength());

        try {
            array.removeThisIndex(-1);
            fail("removing index -1 should throw");
        } catch (Exception e) {
            // expected exception
        }

        try {
            array.removeThisIndex(0);
            fail("removing any index beyond what we have should throw");
        } catch (Exception e) {
            // expected exception
        }

    }

    public void testReset() {
        boolean resetValue = false;
        int nItems = 6;
        FastBooleanArray array = makeArrayWithSingleInitialValue(nItems, !resetValue);
        assertEquals(nItems, array.getLength());

        for (int i = 0; i < nItems; i++) {
            assertFalse(array.getUnsafeArray()[i] == resetValue);
        }

        array.normalReset(resetValue);
        assertEquals(0, array.getLength()); // smokes the length
        for (int i = 0; i < nItems; i++) { // ... but fills in the values to our resetValue
            assertTrue(array.getUnsafeArray()[i] == resetValue);
        }

    }

    public void testDeepCopyAndEquals() {
        boolean val1 = true;

        boolean orig = val1;
        boolean copy = val1;
        assertEquals(orig, copy);

        FastBooleanArray arrayOrig = new FastBooleanArray();
        FastBooleanArray arrayCopy = new FastBooleanArray();

        assertTrue(arrayOrig.equals(arrayCopy));
        assertTrue(arrayCopy.equals(arrayOrig));

        // add something to orig
        arrayOrig.add(orig);
        assertFalse(arrayOrig.equals(arrayCopy));
        assertFalse(arrayCopy.equals(arrayOrig));

        // add another
        boolean other = false;
        arrayOrig.add(other);
        assertFalse(other == orig);
        assertFalse(arrayOrig.equals(arrayCopy));
        assertFalse(arrayCopy.equals(arrayOrig));

        // add those to the copy array
        // just the first item means they should still be not equal
        arrayCopy.add(orig);
        assertFalse(arrayOrig.equals(arrayCopy));
        assertFalse(arrayCopy.equals(arrayOrig));

        // add in all the same stuff so they should be equal
        arrayCopy.add(other);
        assertTrue(arrayOrig.equals(arrayCopy));
        assertTrue(arrayCopy.equals(arrayOrig));
    }

    public void checkExternalization(FastBooleanArray arrayInput, FastBooleanArray arrayReceiver) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            arrayInput.writeExternal(oos);
        } catch (IllegalArgumentException e) {
            if (arrayInput == null) {
                // this is an expected failure
                return;
            } else {
                throw e;
            }
        }
        oos.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bis);
        try {
            arrayReceiver.readExternal(ois);
        } catch (IllegalArgumentException e) {
            if (arrayReceiver == null) {
                // this is an expected failure
                return;
            } else {
                throw e;
            }
        }
        assertTrue(arrayInput.equals(arrayReceiver));
        assertTrue(arrayReceiver.equals(arrayInput));
    }

    public static boolean makeRandomTestItem(Random myRandom) {
        return myRandom.nextFloat() < 0.5 ? true : false;
    }

    public static FastBooleanArray makeArrayWithRandomJunk(int nItems, Random myRandom) {
        if (nItems < 0) {
            return null;
        } else {
            FastBooleanArray result = new FastBooleanArray();
            for (int i = 0; i < nItems; i++) {
                boolean thisItem = makeRandomTestItem(myRandom);
                result.add(thisItem);
            }
            return result;
        }
    }

    public static FastBooleanArray makeArrayWithSingleInitialValue(int nItems, boolean val) {
        if (nItems < 0) {
            return null;
        } else {
            FastBooleanArray result = new FastBooleanArray();
            for (int i = 0; i < nItems; i++) {
                result.add(val);
            }
            return result;
        }
    }

    public void testExternalizationEmptyWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationEmptyWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationEmptyWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastBooleanArray arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationGeneralScan() throws Exception {
        Random myRandom = new Random(88974352L);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                FastBooleanArray arrayInput = makeArrayWithRandomJunk(i, myRandom);
                FastBooleanArray arrayReceiver = makeArrayWithRandomJunk(j, myRandom);
                checkExternalization(arrayInput, arrayReceiver);
            }
        }
    }

    private void checkCopyValuesDeep(int nItems) {
        boolean val = true;
        FastBooleanArray arrayInput = new FastBooleanArray();
        FastBooleanArray arrayReceiver = new FastBooleanArray();
        assertEquals(0, arrayInput.getLength());
        assertEquals(0, arrayReceiver.getLength());

        // put items into input array
        for (int i = 0; i < nItems; i++) {
            arrayInput.add(val);
            assertEquals(i + 1, arrayInput.getLength());
        }
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(0, arrayReceiver.getLength());

        // copy them into receive
        arrayReceiver = arrayInput.safeClone();

        // check the receive array against the input array
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(nItems, arrayReceiver.getLength());
        for (int i = 0; i < nItems; i++) {
            boolean itemInput = arrayInput.getUnsafeArray()[i];
            boolean itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertTrue(itemInput == itemReceive);
        }

        // change the values in the receive array
        for (int i = 0; i < nItems; i++) {
            arrayReceiver.getUnsafeArray()[i] = !val;
        }

        // verify they are not equal
        for (int i = 0; i < nItems; i++) {
            boolean itemInput = arrayInput.getUnsafeArray()[i];
            boolean itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertFalse(itemInput == itemReceive);
        }

        // copy the value from the input back into the receive array (now that we already have values in there
        arrayReceiver = arrayInput.safeClone();

        // check the receive array against the input array
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(nItems, arrayReceiver.getLength());
        for (int i = 0; i < nItems; i++) {
            boolean itemInput = arrayInput.getUnsafeArray()[i];
            boolean itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertTrue(itemInput == itemReceive);
        }
    }

    public void testCopyValuesDeepNoItems() {
        checkCopyValuesDeep(0);
    }

    public void testCopyValuesDeepOneItem() {
        checkCopyValuesDeep(1);
    }

    public void testCopyValuesDeepManyItems() {
        checkCopyValuesDeep(6);
    }

    public void testCopyValuesDeepGeneralScan() {
        for (int nItems = 0; nItems < 10; nItems++) {
            checkCopyValuesDeep(nItems);
        }
    }

    private void checkDeepClone(int nItems) {
        Random myRandom = new Random(89324L);
        FastBooleanArray arrayInput = new FastBooleanArray();
        assertEquals(0, arrayInput.getLength());
        for (int i = 0; i < nItems; i++) {
            arrayInput.add(makeRandomTestItem(myRandom));
            assertEquals(i + 1, arrayInput.getLength());
        }
        assertEquals(nItems, arrayInput.getLength());
        FastBooleanArray arrayReceiver = arrayInput.safeClone();
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(nItems, arrayReceiver.getLength());
        for (int i = 0; i < nItems; i++) {
            boolean itemInput = arrayInput.getUnsafeArray()[i];
            boolean itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertTrue(itemInput == itemReceive);
        }
    }

    public void testDeepCloneNoItems() {
        checkDeepClone(0);
    }

    public void testDeepCloneOneItem() {
        checkDeepClone(1);
    }

    public void testDeepCloneManyItems() {
        checkDeepClone(8);
    }

    public void testDeepCloneManyItemsGeneralScan() {
        for (int nItems = 0; nItems < 10; nItems++) {
            checkDeepClone(nItems);
        }
    }

}


