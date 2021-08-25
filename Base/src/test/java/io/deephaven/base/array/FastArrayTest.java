/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.array;

import junit.framework.TestCase;

import java.io.*;
import java.util.Random;

public class FastArrayTest extends TestCase {

    public void testAdd() {
        FastArray<TrialClassA> array = new FastArray<TrialClassA>(TrialClassA.class);
        assertEquals(0, array.getLength());

        Random myRandom = new Random(89324L);

        TrialClassA item1 = makeRandomTestObject(myRandom);
        array.add(item1);
        assertEquals(1, array.getLength());
        assertTrue(array.getUnsafeArray()[0].equals(item1));

        TrialClassA item2 = makeRandomTestObject(myRandom);
        array.add(item2);
        assertEquals(2, array.getLength());
        assertTrue(array.getUnsafeArray()[1].equals(item2));

        TrialClassA item3 = makeRandomTestObject(myRandom);
        array.add(item3);
        assertEquals(3, array.getLength());
        assertTrue(array.getUnsafeArray()[2].equals(item3));

        TrialClassA item4 = makeRandomTestObject(myRandom);
        array.add(item4);
        assertEquals(4, array.getLength());
        assertTrue(array.getUnsafeArray()[3].equals(item4));

        TrialClassA item5 = makeRandomTestObject(myRandom);
        array.add(item5);
        assertEquals(5, array.getLength());
        assertTrue(array.getUnsafeArray()[4].equals(item5));

        array.quickReset();
        assertEquals(0, array.getLength());
    }

    public void testRemove() {
        FastArray<TrialClassA> array = new FastArray<TrialClassA>(TrialClassA.class);
        assertEquals(0, array.getLength());

        Random myRandom = new Random(89324L);

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
        TrialClassA item1 = makeRandomTestObject(myRandom);
        array.add(item1);
        assertEquals(1, array.getLength());
        assertTrue(array.getUnsafeArray()[0].equals(item1));


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
        TrialClassA item2 = makeRandomTestObject(myRandom);
        array.add(item2);
        assertEquals(2, array.getLength());
        assertTrue(array.getUnsafeArray()[1].equals(item2));

        TrialClassA item3 = makeRandomTestObject(myRandom);
        array.add(item3);
        assertEquals(3, array.getLength());
        assertTrue(array.getUnsafeArray()[2].equals(item3));

        TrialClassA item4 = makeRandomTestObject(myRandom);
        array.add(item4);
        assertEquals(4, array.getLength());
        assertTrue(array.getUnsafeArray()[3].equals(item4));

        TrialClassA item5 = makeRandomTestObject(myRandom);
        array.add(item5);
        assertEquals(5, array.getLength());
        assertTrue(array.getUnsafeArray()[4].equals(item5));

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
        assertTrue(array.getUnsafeArray()[0].equals(item2));
        assertTrue(array.getUnsafeArray()[1].equals(item3));
        assertTrue(array.getUnsafeArray()[2].equals(item4));
        assertTrue(array.getUnsafeArray()[3].equals(item5));

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
        assertTrue(array.getUnsafeArray()[0].equals(item2));
        assertTrue(array.getUnsafeArray()[1].equals(item3));
        assertTrue(array.getUnsafeArray()[2].equals(item5));

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
        assertTrue(array.getUnsafeArray()[0].equals(item2));
        assertTrue(array.getUnsafeArray()[1].equals(item5));

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
        assertTrue(array.getUnsafeArray()[0].equals(item2));

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
        Random myRandom = new Random(88974352L);
        int nItems = 6;
        FastArray<TrialClassA> array = makeArrayWithRandomJunk(nItems, myRandom);
        assertEquals(nItems, array.getLength());
        TrialClassA resetValue = makeRandomTestObject(myRandom);

        for (int i = 0; i < nItems; i++) {
            assertFalse(array.getUnsafeArray()[i].equals(resetValue));
            assertFalse(resetValue.equals(array.getUnsafeArray()[i]));
        }

        array.normalReset(resetValue);
        assertEquals(0, array.getLength()); // smokes the length
        for (int i = 0; i < nItems; i++) { // ... but fills in the values to our resetValue
            assertTrue(array.getUnsafeArray()[i].equals(resetValue));
            assertTrue(resetValue.equals(array.getUnsafeArray()[i]));
        }

    }

    public void testDeepCopyAndEquals() {
        double double1 = 12.9;
        int int1 = 3;
        long long1 = 897234897L;

        TrialClassA orig = new TrialClassA(double1, int1, long1);

        double tol = 1e-14;
        assertEquals(double1, orig.getDouble1(), tol);
        assertEquals(int1, orig.getInt1());
        assertEquals(long1, orig.getLong1());

        TrialClassA copy = orig.deepCopy();

        assertEquals(double1, copy.getDouble1(), tol);
        assertEquals(int1, copy.getInt1());
        assertEquals(long1, copy.getLong1());
        assertTrue(orig.equals(copy));
        assertTrue(copy.equals(orig)); // paranoid of some assymetry in the equals

        // make the state more messy
        copy.setDouble1(double1 + 0.1);
        assertFalse(orig.equals(copy));
        assertFalse(copy.equals(orig));
        copy.setDouble1(double1);
        assertTrue(orig.equals(copy));
        assertTrue(copy.equals(orig));

        copy.setInt1(int1 + 1);
        assertFalse(orig.equals(copy));
        assertFalse(copy.equals(orig));
        copy.setInt1(int1);
        assertTrue(orig.equals(copy));
        assertTrue(copy.equals(orig));

        copy.setLong1(long1 + 1);
        assertFalse(orig.equals(copy));
        assertFalse(copy.equals(orig));
        copy.setLong1(long1);
        assertTrue(orig.equals(copy));
        assertTrue(copy.equals(orig));


        // make it a real copy again
        copy = orig.deepCopy();
        assertTrue(orig.equals(copy));
        assertTrue(copy.equals(orig));

        copy.setDouble1(double1 + 0.1);
        assertFalse(orig.equals(copy));
        assertFalse(copy.equals(orig));

        // now we know the test object is behaving itself

        FastArray<TrialClassA> arrayOrig = new FastArray<TrialClassA>(TrialClassA.class);
        FastArray<TrialClassA> arrayCopy = new FastArray<TrialClassA>(TrialClassA.class);

        assertTrue(arrayOrig.equals(arrayCopy));
        assertTrue(arrayCopy.equals(arrayOrig));

        // add something to orig
        arrayOrig.add(orig);
        assertFalse(arrayOrig.equals(arrayCopy));
        assertFalse(arrayCopy.equals(arrayOrig));

        // add another
        TrialClassA other = orig.deepCopy();
        other.setDouble1(double1 + 0.4);
        other.setInt1(int1 + 2);
        other.setLong1(long1 + 65);
        arrayOrig.add(other);
        assertFalse(other.equals(orig));
        assertFalse(orig.equals(other));
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

    public void checkExternalization(FastArray<TrialClassA> arrayInput,
        FastArray<TrialClassA> arrayReceiver) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            FastArray.writeExternal(arrayInput, oos, TrialClassA.getWriter());
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
            FastArray.readExternal(arrayReceiver, ois, TrialClassA.getReader());
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

    public static TrialClassA makeRandomTestObject(Random myRandom) {
        return new TrialClassA(myRandom.nextDouble(), myRandom.nextInt(), myRandom.nextLong());
    }

    public static FastArray<TrialClassA> makeArrayWithRandomJunk(int nItems, Random myRandom) {
        if (nItems < 0) {
            return null;
        } else {
            FastArray<TrialClassA> result = new FastArray<TrialClassA>(TrialClassA.class);
            for (int i = 0; i < nItems; i++) {
                TrialClassA thisItem = makeRandomTestObject(myRandom);
                // System.out.println("random item look ok? " + i + " " + thisItem);
                result.add(thisItem);
            }
            return result;
        }
    }

    public void testExternalizationNullWithNullReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(-1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(-1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationEmptyWithNullReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(-1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithNullReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(-1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithNullReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(-1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationNullWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(-1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationEmptyWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithEmptyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(0, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationNullWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(-1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationEmptyWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithOneReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(1, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationNullWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(-1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationEmptyWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(0, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationOneItemWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(1, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationManyItemsWithManyReceiver() throws Exception {
        Random myRandom = new Random(88974352L);
        FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(6, myRandom);
        FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(8, myRandom);
        checkExternalization(arrayInput, arrayReceiver);
    }

    public void testExternalizationGeneralScan() throws Exception {
        Random myRandom = new Random(88974352L);
        for (int i = -1; i < 10; i++) {
            for (int j = -1; j < 10; j++) {
                FastArray<TrialClassA> arrayInput = makeArrayWithRandomJunk(i, myRandom);
                FastArray<TrialClassA> arrayReceiver = makeArrayWithRandomJunk(j, myRandom);
                checkExternalization(arrayInput, arrayReceiver);
            }
        }
    }

    private void checkCopyValuesDeep(int nItems) {
        Random myRandom = new Random(89324L);
        FastArray<TrialClassA> arrayInput = new FastArray<TrialClassA>(TrialClassA.class);
        FastArray<TrialClassA> arrayReceiver = new FastArray<TrialClassA>(TrialClassA.class);
        assertEquals(0, arrayInput.getLength());
        assertEquals(0, arrayReceiver.getLength());

        // put items into input array
        for (int i = 0; i < nItems; i++) {
            arrayInput.add(makeRandomTestObject(myRandom));
            assertEquals(i + 1, arrayInput.getLength());
        }
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(0, arrayReceiver.getLength());

        // copy them into receive
        FastArray.copyValuesDeep(arrayReceiver, arrayInput);

        // check the receive array against the input array
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(nItems, arrayReceiver.getLength());
        for (int i = 0; i < nItems; i++) {
            TrialClassA itemInput = arrayInput.getUnsafeArray()[i];
            TrialClassA itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertTrue(itemInput.equals(itemReceive));
            assertTrue(itemReceive.equals(itemInput));
        }

        // change the values in the receive array
        for (int i = 0; i < nItems; i++) {
            arrayReceiver.getUnsafeArray()[i] = makeRandomTestObject(myRandom);
        }

        // verify they are not equal
        for (int i = 0; i < nItems; i++) {
            TrialClassA itemInput = arrayInput.getUnsafeArray()[i];
            TrialClassA itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertFalse(itemInput.equals(itemReceive));
            assertFalse(itemReceive.equals(itemInput));
        }

        // copy the value from the input back into the receive array (now that we already have
        // values in there
        FastArray.copyValuesDeep(arrayReceiver, arrayInput);

        // check the receive array against the input array
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(nItems, arrayReceiver.getLength());
        for (int i = 0; i < nItems; i++) {
            TrialClassA itemInput = arrayInput.getUnsafeArray()[i];
            TrialClassA itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertTrue(itemInput.equals(itemReceive));
            assertTrue(itemReceive.equals(itemInput));
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
        FastArray<TrialClassA> arrayInput = new FastArray<TrialClassA>(TrialClassA.class);
        assertEquals(0, arrayInput.getLength());
        for (int i = 0; i < nItems; i++) {
            arrayInput.add(makeRandomTestObject(myRandom));
            assertEquals(i + 1, arrayInput.getLength());
        }
        assertEquals(nItems, arrayInput.getLength());
        FastArray<TrialClassA> arrayReceiver = FastArray.cloneDeep(arrayInput);
        assertEquals(nItems, arrayInput.getLength());
        assertEquals(nItems, arrayReceiver.getLength());
        for (int i = 0; i < nItems; i++) {
            TrialClassA itemInput = arrayInput.getUnsafeArray()[i];
            TrialClassA itemReceive = arrayReceiver.getUnsafeArray()[i];
            assertTrue(itemInput.equals(itemReceive));
            assertTrue(itemReceive.equals(itemInput));
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


