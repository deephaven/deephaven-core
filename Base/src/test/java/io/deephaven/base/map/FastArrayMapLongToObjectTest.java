/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.map;

import io.deephaven.base.Function;
import io.deephaven.base.array.FastArrayTest;
import io.deephaven.base.array.TrialClassA;
import junit.framework.TestCase;

import java.io.*;
import java.util.Random;

public class FastArrayMapLongToObjectTest extends TestCase {

    private FastArrayMapLongToObject<TrialClassA> constructTestMap() {
        Function.Nullary<KeyValuePairLongToObject<TrialClassA>> factoryLongToObject = new FactoryLongToObject();
        FastArrayMapLongToObject<TrialClassA> map = new FastArrayMapLongToObject<TrialClassA>(factoryLongToObject);
        return map;
    }

    private FastArrayMapLongToObject<TrialClassA> constructTestMap(int nItems, Random myRandom) {
        FastArrayMapLongToObject<TrialClassA> map = constructTestMap();
        for (int i = 0; i < nItems; i++) {
            long key = myRandom.nextLong();
            TrialClassA value = FastArrayTest.makeRandomTestObject(myRandom);
            map.put(key, value);
        }
        return map;
    }

    private static long dummyKey1 = 913L;
    private static long dummyKey2 = 993L;

    public void testAdding() {
        FastArrayMapLongToObject<TrialClassA> map = constructTestMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.containsKey(dummyKey2));

        Random myRandom = new Random(454546L);

        // add a value
        long key1 = 1L;
        TrialClassA value1 = FastArrayTest.makeRandomTestObject(myRandom);
        TrialClassA prevValue = map.put(key1, value1);
        assertTrue(prevValue == null);
        assertEquals(1, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.get(key1).equals(value1));

        // repeat adding this same item again
        prevValue = map.put(key1, value1);
        assertTrue(prevValue.equals(value1));
        assertEquals(1, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.get(key1).equals(value1));

        // add a value
        long key2 = 2L;
        TrialClassA value2 = FastArrayTest.makeRandomTestObject(myRandom);
        prevValue = map.put(key2, value2);
        assertTrue(prevValue == null);
        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));

        // add this value again
        prevValue = map.put(key2, value2);
        assertTrue(prevValue.equals(value2));
        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));

        // add the 1st item again
        prevValue = map.put(key1, value1);
        assertTrue(prevValue.equals(value1));
        assertEquals(2, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));

        // add a value
        long key10 = 10L;
        TrialClassA value10 = FastArrayTest.makeRandomTestObject(myRandom);
        prevValue = map.put(key10, value10);
        assertTrue(prevValue == null);
        assertEquals(3, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.get(key10) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.containsKey(key10));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key10).equals(value10));

        // add a value out of key order
        long key5 = 5L;
        TrialClassA value5 = FastArrayTest.makeRandomTestObject(myRandom);
        prevValue = map.put(key5, value5);
        assertTrue(prevValue == null);
        assertEquals(4, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.get(key5) == null);
        assertFalse(map.get(key10) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.containsKey(key5));
        assertTrue(map.containsKey(key10));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(key10).equals(value10));

        // add the 5 item again
        prevValue = map.put(key5, value5);
        assertTrue(prevValue.equals(value5));
        assertEquals(4, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.get(key5) == null);
        assertFalse(map.get(key10) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.containsKey(key5));
        assertTrue(map.containsKey(key10));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(key10).equals(value10));

        // add the 10 item again
        prevValue = map.put(key10, value10);
        assertTrue(prevValue.equals(value10));
        assertEquals(4, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.get(key5) == null);
        assertFalse(map.get(key10) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.containsKey(key5));
        assertTrue(map.containsKey(key10));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(key10).equals(value10));

        // add the 1 item again
        prevValue = map.put(key1, value1);
        assertTrue(prevValue.equals(value1));
        assertEquals(4, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.get(key5) == null);
        assertFalse(map.get(key10) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.containsKey(key5));
        assertTrue(map.containsKey(key10));
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(key10).equals(value10));

        // add value2 into key1
        prevValue = map.put(key1, value2);
        assertTrue(prevValue.equals(value1));
        assertEquals(4, map.size());
        assertFalse(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.get(key1) == null);
        assertFalse(map.get(key2) == null);
        assertFalse(map.get(key5) == null);
        assertFalse(map.get(key10) == null);
        assertFalse(map.containsKey(dummyKey2));
        assertTrue(map.containsKey(key1));
        assertTrue(map.containsKey(key2));
        assertTrue(map.containsKey(key5));
        assertTrue(map.containsKey(key10));
        assertTrue(map.get(key1).equals(value2));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(key10).equals(value10));
    }

    public void testClear() {
        FastArrayMapLongToObject<TrialClassA> map = constructTestMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.containsKey(dummyKey2));

        Random myRandom = new Random(454546L);

        // add a bunch of values
        long key1 = 1L;
        TrialClassA value1 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key1, value1);
        long key2 = 2L;
        TrialClassA value2 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key2, value2);
        long key3 = 3L;
        TrialClassA value3 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key3, value3);
        long key4 = 4L;
        TrialClassA value4 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key4, value4);
        long key5 = 5L;
        TrialClassA value5 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key5, value5);

        assertEquals(5, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3).equals(value3));
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(6L) == null);

        // clear it and see what is left
        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4) == null);
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(6L) == null);
    }

    public void testRemove() {
        FastArrayMapLongToObject<TrialClassA> map = constructTestMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.containsKey(dummyKey2));

        Random myRandom = new Random(454546L);

        // add a bunch of values
        long key1 = 1L;
        TrialClassA value1 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key1, value1);
        long key2 = 2L;
        TrialClassA value2 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key2, value2);
        long key3 = 3L;
        TrialClassA value3 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key3, value3);
        long key4 = 4L;
        TrialClassA value4 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key4, value4);
        long key5 = 5L;
        TrialClassA value5 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key5, value5);

        assertEquals(5, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3).equals(value3));
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(6L) == null);

        // delete something and see what is left
        TrialClassA prevValue = map.remove(key3);
        assertTrue(prevValue.equals(value3));
        assertEquals(4, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(6L) == null);

        // delete the same item again
        prevValue = map.remove(key3);
        assertTrue(prevValue == null);
        assertEquals(4, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(6L) == null);

        prevValue = map.remove(key1);
        assertTrue(prevValue.equals(value1));
        assertEquals(3, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(6L) == null);

        prevValue = map.remove(key5);
        assertTrue(prevValue.equals(value5));
        assertEquals(2, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(6L) == null);

        prevValue = map.remove(key2);
        assertTrue(prevValue.equals(value2));
        assertEquals(1, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(6L) == null);

        // remove final item
        prevValue = map.remove(key4);
        assertTrue(prevValue.equals(value4));
        assertEquals(0, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4) == null);
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(6L) == null);

        // remove something from an empty map
        prevValue = map.remove(key4);
        assertTrue(prevValue == null);
        assertEquals(0, map.size());
        assertTrue(map.get(0L) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4) == null);
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(6L) == null);
    }

    public void checkExternalization(FastArrayMapLongToObject<TrialClassA> mapInput,
            FastArrayMapLongToObject<TrialClassA> mapReceiver) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            FastArrayMapLongToObject.writeExternal(mapInput, oos, KeyValuePairLongToObjectTest.writer);
        } catch (IllegalArgumentException e) {
            if (mapInput == null) {
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
            FastArrayMapLongToObject.readExternal(mapReceiver, ois, KeyValuePairLongToObjectTest.reader);
        } catch (IllegalArgumentException e) {
            if (mapReceiver == null) {
                // this is an expected failure
                return;
            } else {
                throw e;
            }
        }

        assertTrue(mapInput.equals(mapReceiver));
        assertTrue(mapReceiver.equals(mapInput));
    }

    private void checkExternalization(int nInput, int nReceiver) throws Exception {
        Random myRandom = new Random(454546L);
        FastArrayMapLongToObject<TrialClassA> mapInput = constructTestMap(nInput, myRandom);
        FastArrayMapLongToObject<TrialClassA> mapReceiver = constructTestMap(nReceiver, myRandom);
        checkExternalization(mapInput, mapReceiver);
    }

    public void testExternalizationInput0Receiver0() throws Exception {
        checkExternalization(0, 0);
    }

    public void testExternalizationInput1Receiver0() throws Exception {
        checkExternalization(1, 0);
    }

    public void testExternalizationInputManyReceiver0() throws Exception {
        checkExternalization(6, 0);
    }

    public void testExternalizationInput0Receiver1() throws Exception {
        checkExternalization(0, 1);
    }

    public void testExternalizationInput1Receiver1() throws Exception {
        checkExternalization(1, 1);
    }

    public void testExternalizationInputManyReceiver1() throws Exception {
        checkExternalization(6, 1);
    }

    public void testExternalizationInput0ReceiverMany() throws Exception {
        checkExternalization(0, 7);
    }

    public void testExternalizationInput1ReceiverMany() throws Exception {
        checkExternalization(1, 7);
    }

    public void testExternalizationInputManyReceiverMany() throws Exception {
        checkExternalization(6, 5);
        checkExternalization(6, 6);
        checkExternalization(6, 7);
    }

    public void testExternalizationScan() throws Exception {
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 10; j++) {
                checkExternalization(i, j);
            }
        }
    }

    private void checkCopyValuesDeep(int nItems) {
        Random myRandom = new Random(89324L);
        FastArrayMapLongToObject<TrialClassA> arrayInput = constructTestMap(0, myRandom);
        FastArrayMapLongToObject<TrialClassA> arrayReceiver = constructTestMap(0, myRandom);
        assertEquals(0, arrayInput.size());
        assertEquals(0, arrayReceiver.size());

        // put items into input array
        for (int i = 0; i < nItems; i++) {
            long key = myRandom.nextLong();
            arrayInput.put(key, FastArrayTest.makeRandomTestObject(myRandom));
            assertEquals(i + 1, arrayInput.size());
        }
        assertEquals(nItems, arrayInput.size());
        assertEquals(0, arrayReceiver.size());

        // copy them into receive
        arrayReceiver = arrayInput.cloneDeep();

        // check the receive array against the input array
        assertEquals(nItems, arrayInput.size());
        assertEquals(nItems, arrayReceiver.size());
        for (int i = 0; i < nItems; i++) {
            long keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            long keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertEquals(keyInput, keyReceive);
            assertEquals(keyReceive, keyInput);
            TrialClassA itemInput = arrayInput.getArray().getUnsafeArray()[i].getValue();
            TrialClassA itemReceive = arrayReceiver.getArray().getUnsafeArray()[i].getValue();
            assertTrue(itemInput.equals(itemReceive));
            assertTrue(itemReceive.equals(itemInput));
        }

        // change the values in the receive array
        for (int i = 0; i < nItems; i++) {
            long key = myRandom.nextLong();
            TrialClassA val = FastArrayTest.makeRandomTestObject(myRandom);
            arrayReceiver.getArray().getUnsafeArray()[i] = new KeyValuePairLongToObject<TrialClassA>(key, val);
        }

        // verify they are not equal
        for (int i = 0; i < nItems; i++) {
            long keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            long keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertFalse(keyInput == keyReceive);
            assertFalse(keyReceive == keyInput);
            TrialClassA itemInput = arrayInput.getArray().getUnsafeArray()[i].getValue();
            TrialClassA itemReceive = arrayReceiver.getArray().getUnsafeArray()[i].getValue();
            assertFalse(itemInput.equals(itemReceive));
            assertFalse(itemReceive.equals(itemInput));
        }

        // copy the value from the input back into the receive array (now that we already have values in there
        arrayReceiver = arrayInput.cloneDeep();

        // check the receive array against the input array
        assertEquals(nItems, arrayInput.size());
        assertEquals(nItems, arrayReceiver.size());
        for (int i = 0; i < nItems; i++) {
            long keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            long keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertEquals(keyInput, keyReceive);
            assertEquals(keyReceive, keyInput);
            TrialClassA itemInput = arrayInput.getArray().getUnsafeArray()[i].getValue();
            TrialClassA itemReceive = arrayReceiver.getArray().getUnsafeArray()[i].getValue();
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
        FastArrayMapLongToObject<TrialClassA> arrayInput = constructTestMap(0, myRandom);
        assertEquals(0, arrayInput.size());
        for (int i = 0; i < nItems; i++) {
            long key = myRandom.nextLong();
            arrayInput.put(key, FastArrayTest.makeRandomTestObject(myRandom));
            assertEquals(i + 1, arrayInput.size());
        }
        assertEquals(nItems, arrayInput.size());
        FastArrayMapLongToObject<TrialClassA> arrayReceiver = arrayInput.cloneDeep();
        assertEquals(nItems, arrayInput.size());
        assertEquals(nItems, arrayReceiver.size());
        for (int i = 0; i < nItems; i++) {
            long keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            long keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertEquals(keyInput, keyReceive);
            assertEquals(keyReceive, keyInput);
            TrialClassA itemInput = arrayInput.getArray().getUnsafeArray()[i].getValue();
            TrialClassA itemReceive = arrayReceiver.getArray().getUnsafeArray()[i].getValue();
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

    public void testCompareToBaseFastArrayMap() {
        Function.Nullary<KeyValuePairLongToObject<TrialClassA>> factoryLongToObject = new FactoryLongToObject();
        FastArrayMapLongToObject<TrialClassA> mapLongToObject =
                new FastArrayMapLongToObject<TrialClassA>(factoryLongToObject);

        Function.Nullary<KeyValuePair<LongWrapper, TrialClassA>> factoryObjectToObject = new FactoryObjectToObject();
        FastArrayMap<LongWrapper, TrialClassA> mapObjectToObject =
                new FastArrayMap<LongWrapper, TrialClassA>(factoryObjectToObject);

        Random myRandom = new Random(98723498L);
        double clearFraction = 0.03;
        double removeFraction = 0.2;
        double addFraction = 0.9;
        int nTestValues = 1000;
        int maxKey = 30;
        for (int i = 0; i < nTestValues; i++) {

            // randomly add something
            if (myRandom.nextFloat() < addFraction) {
                // System.out.println(i+" ADD");
                long key = myRandom.nextInt(maxKey); // get a key between 0 and maxKey
                TrialClassA value = new TrialClassA(myRandom.nextDouble(), myRandom.nextInt(), myRandom.nextLong());
                mapObjectToObject.put(new LongWrapper(key), value);
                mapLongToObject.put(key, value);
            }

            // randomly remove stuff
            if (myRandom.nextFloat() < removeFraction) {
                int length = mapObjectToObject.getArray().getLength();
                if (length > 0) {
                    // pick item to remove
                    int indexToRemove = 0;
                    if (length > 1) {
                        indexToRemove = myRandom.nextInt(length - 1);
                    }
                    long key = mapLongToObject.getArray().getUnsafeArray()[indexToRemove].getKey();

                    // remove it
                    // System.out.println(i+" REMOVE");
                    mapObjectToObject.remove(new LongWrapper(key));
                    mapLongToObject.remove(key);
                }
            }

            // randomly clear
            if (myRandom.nextFloat() < clearFraction) {
                // System.out.println(i+" CLEAR");
                mapObjectToObject.clear();
                mapLongToObject.clear();
            }

            // System.out.println(i+" SIZE OF MAPS: "+mapLongToObject.size());

            // check the maps have the same content
            assertEquals(mapObjectToObject.size(), mapLongToObject.size());
            assertEquals(mapObjectToObject.isEmpty(), mapLongToObject.isEmpty());
            for (int j = 0; j < mapObjectToObject.size(); j++) {
                KeyValuePair<LongWrapper, TrialClassA> refPair = mapObjectToObject.getArray().getUnsafeArray()[j];
                KeyValuePairLongToObject<TrialClassA> trialPair = mapLongToObject.getArray().getUnsafeArray()[j];
                assertEquals(refPair.getKey().getVal(), trialPair.getKey());
                assertTrue(refPair.getValue().equals(trialPair.getValue()));
            }
        }

    }

}


class FactoryLongToObject implements Function.Nullary<KeyValuePairLongToObject<TrialClassA>> {

    @Override
    public KeyValuePairLongToObject<TrialClassA> call() {
        long key = Long.MIN_VALUE;
        TrialClassA value = TrialClassA.makeNull();
        KeyValuePairLongToObject<TrialClassA> result = new KeyValuePairLongToObject<TrialClassA>(key, value);
        return result;
    }
}
