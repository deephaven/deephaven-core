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

public class FastArrayMapTest extends TestCase {

    private FastArrayMap<LongWrapper, TrialClassA> constructTestMap() {
        Function.Nullary<KeyValuePair<LongWrapper, TrialClassA>> factoryObjectToObject = new FactoryObjectToObject();
        FastArrayMap<LongWrapper, TrialClassA> map = new FastArrayMap<LongWrapper, TrialClassA>(factoryObjectToObject);
        return map;
    }

    private FastArrayMap<LongWrapper, TrialClassA> constructTestMap(int nItems, Random myRandom) {
        FastArrayMap<LongWrapper, TrialClassA> map = constructTestMap();
        for (int i = 0; i < nItems; i++) {
            LongWrapper key = new LongWrapper(myRandom.nextLong());
            TrialClassA value = FastArrayTest.makeRandomTestObject(myRandom);
            map.put(key, value);
        }
        return map;
    }

    private static LongWrapper dummyKey1 = new LongWrapper(913L);
    private static LongWrapper dummyKey2 = new LongWrapper(993L);

    public void testAdding() {
        FastArrayMap<LongWrapper, TrialClassA> map = constructTestMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.containsKey(dummyKey2));

        Random myRandom = new Random(454546L);

        // add a value
        LongWrapper key1 = new LongWrapper(1L);
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
        LongWrapper key2 = new LongWrapper(2L);
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
        LongWrapper key10 = new LongWrapper(10L);
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
        LongWrapper key5 = new LongWrapper(5L);
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
        FastArrayMap<LongWrapper, TrialClassA> map = constructTestMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.containsKey(dummyKey2));

        Random myRandom = new Random(454546L);

        // add a bunch of values
        LongWrapper key1 = new LongWrapper(1L);
        TrialClassA value1 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key1, value1);
        LongWrapper key2 = new LongWrapper(2L);
        TrialClassA value2 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key2, value2);
        LongWrapper key3 = new LongWrapper(3L);
        TrialClassA value3 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key3, value3);
        LongWrapper key4 = new LongWrapper(4L);
        TrialClassA value4 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key4, value4);
        LongWrapper key5 = new LongWrapper(5L);
        TrialClassA value5 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key5, value5);

        assertEquals(5, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3).equals(value3));
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(new LongWrapper(6L)) == null);

        // clear it and see what is left
        map.clear();
        assertEquals(0, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4) == null);
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(new LongWrapper(6L)) == null);
    }

    public void testRemove() {
        FastArrayMap<LongWrapper, TrialClassA> map = constructTestMap();
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
        assertTrue(map.get(dummyKey1) == null);
        assertFalse(map.containsKey(dummyKey2));

        Random myRandom = new Random(454546L);

        // add a bunch of values
        LongWrapper key1 = new LongWrapper(1L);
        TrialClassA value1 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key1, value1);
        LongWrapper key2 = new LongWrapper(2L);
        TrialClassA value2 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key2, value2);
        LongWrapper key3 = new LongWrapper(3L);
        TrialClassA value3 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key3, value3);
        LongWrapper key4 = new LongWrapper(4L);
        TrialClassA value4 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key4, value4);
        LongWrapper key5 = new LongWrapper(5L);
        TrialClassA value5 = FastArrayTest.makeRandomTestObject(myRandom);
        map.put(key5, value5);

        assertEquals(5, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3).equals(value3));
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(new LongWrapper(6L)) == null);

        // delete something and see what is left
        TrialClassA prevValue = map.remove(key3);
        assertTrue(prevValue.equals(value3));
        assertEquals(4, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(new LongWrapper(6L)) == null);

        // delete the same item again
        prevValue = map.remove(key3);
        assertTrue(prevValue == null);
        assertEquals(4, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1).equals(value1));
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(new LongWrapper(6L)) == null);

        prevValue = map.remove(key1);
        assertTrue(prevValue.equals(value1));
        assertEquals(3, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5).equals(value5));
        assertTrue(map.get(new LongWrapper(6L)) == null);

        prevValue = map.remove(key5);
        assertTrue(prevValue.equals(value5));
        assertEquals(2, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2).equals(value2));
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(new LongWrapper(6L)) == null);

        prevValue = map.remove(key2);
        assertTrue(prevValue.equals(value2));
        assertEquals(1, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4).equals(value4));
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(new LongWrapper(6L)) == null);

        // remove final item
        prevValue = map.remove(key4);
        assertTrue(prevValue.equals(value4));
        assertEquals(0, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4) == null);
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(new LongWrapper(6L)) == null);

        // remove something from an empty map
        prevValue = map.remove(key4);
        assertTrue(prevValue == null);
        assertEquals(0, map.size());
        assertTrue(map.get(new LongWrapper(0L)) == null);
        assertTrue(map.get(key1) == null);
        assertTrue(map.get(key2) == null);
        assertTrue(map.get(key3) == null);
        assertTrue(map.get(key4) == null);
        assertTrue(map.get(key5) == null);
        assertTrue(map.get(new LongWrapper(6L)) == null);
    }

    public void checkExternalization(FastArrayMap<LongWrapper, TrialClassA> mapInput,
            FastArrayMap<LongWrapper, TrialClassA> mapReceiver) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        try {
            FastArrayMap.writeExternal(mapInput, oos, KeyValuePairTest.writer);
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
            FastArrayMap.readExternal(mapReceiver, ois, KeyValuePairTest.reader);
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
        FastArrayMap<LongWrapper, TrialClassA> mapInput = constructTestMap(nInput, myRandom);
        FastArrayMap<LongWrapper, TrialClassA> mapReceiver = constructTestMap(nReceiver, myRandom);
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
        FastArrayMap<LongWrapper, TrialClassA> arrayInput = constructTestMap(0, myRandom);
        FastArrayMap<LongWrapper, TrialClassA> arrayReceiver = constructTestMap(0, myRandom);
        assertEquals(0, arrayInput.size());
        assertEquals(0, arrayReceiver.size());

        // put items into input array
        for (int i = 0; i < nItems; i++) {
            LongWrapper key = new LongWrapper(myRandom.nextLong());
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
            LongWrapper keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            LongWrapper keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertTrue(keyInput.equals(keyReceive));
            assertTrue(keyReceive.equals(keyInput));
            TrialClassA itemInput = arrayInput.getArray().getUnsafeArray()[i].getValue();
            TrialClassA itemReceive = arrayReceiver.getArray().getUnsafeArray()[i].getValue();
            assertTrue(itemInput.equals(itemReceive));
            assertTrue(itemReceive.equals(itemInput));
        }

        // change the values in the receive array
        for (int i = 0; i < nItems; i++) {
            LongWrapper key = new LongWrapper(myRandom.nextLong());
            TrialClassA val = FastArrayTest.makeRandomTestObject(myRandom);
            arrayReceiver.getArray().getUnsafeArray()[i] = new KeyValuePair<LongWrapper, TrialClassA>(key, val);
        }

        // verify they are not equal
        for (int i = 0; i < nItems; i++) {
            LongWrapper keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            LongWrapper keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertFalse(keyInput.equals(keyReceive));
            assertFalse(keyReceive.equals(keyInput));
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
            LongWrapper keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            LongWrapper keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertTrue(keyInput.equals(keyReceive));
            assertTrue(keyReceive.equals(keyInput));
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
        FastArrayMap<LongWrapper, TrialClassA> arrayInput = constructTestMap(0, myRandom);
        assertEquals(0, arrayInput.size());
        for (int i = 0; i < nItems; i++) {
            LongWrapper key = new LongWrapper(myRandom.nextLong());
            arrayInput.put(key, FastArrayTest.makeRandomTestObject(myRandom));
            assertEquals(i + 1, arrayInput.size());
        }
        assertEquals(nItems, arrayInput.size());
        FastArrayMap<LongWrapper, TrialClassA> arrayReceiver = arrayInput.cloneDeep();
        assertEquals(nItems, arrayInput.size());
        assertEquals(nItems, arrayReceiver.size());
        for (int i = 0; i < nItems; i++) {
            LongWrapper keyInput = arrayInput.getArray().getUnsafeArray()[i].getKey();
            LongWrapper keyReceive = arrayReceiver.getArray().getUnsafeArray()[i].getKey();
            assertTrue(keyInput.equals(keyReceive));
            assertTrue(keyReceive.equals(keyInput));
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
}


class FactoryObjectToObject implements Function.Nullary<KeyValuePair<LongWrapper, TrialClassA>> {

    @Override
    public KeyValuePair<LongWrapper, TrialClassA> call() {
        LongWrapper key = new LongWrapper(Long.MIN_VALUE);
        TrialClassA value = TrialClassA.makeNull();
        KeyValuePair<LongWrapper, TrialClassA> result = new KeyValuePair<LongWrapper, TrialClassA>(key, value);
        return result;
    }
}

