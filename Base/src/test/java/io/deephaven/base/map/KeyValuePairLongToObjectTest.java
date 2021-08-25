/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.map;

import io.deephaven.base.array.FastArray;
import io.deephaven.base.array.FastArrayTest;
import io.deephaven.base.array.TrialClassA;
import junit.framework.TestCase;

import java.io.*;
import java.util.Random;

public class KeyValuePairLongToObjectTest extends TestCase {

    private static KeyValuePairLongToObject<TrialClassA> makeRandomKvp(Random myRandom) {
        long key = myRandom.nextLong();
        TrialClassA value = FastArrayTest.makeRandomTestObject(myRandom);
        return new KeyValuePairLongToObject<TrialClassA>(key, value);
    }


    private static class Reader implements FastArray.ReadExternalFunction<KeyValuePairLongToObject<TrialClassA>> {

        @Override
        public void readExternal(ObjectInput in, KeyValuePairLongToObject<TrialClassA> item)
                throws IOException, ClassNotFoundException {
            byte nullByteA = in.readByte();
            // System.out.println("read nullByteA: " + nullByteA);
            if (nullByteA == 1) {
                // System.out.println("item is null");
                item = null;
            } else if (nullByteA == 0) {

                // key
                long key = in.readLong();
                // System.out.println("read key: "+key");
                item.setKey(key);

                // value
                byte nullByteC = in.readByte();
                // System.out.println("read nullByteC: " + nullByteC);
                if (nullByteC == 1) {
                    // System.out.println("item.value is null");
                    item.setValue(null);
                } else if (nullByteC == 0) {
                    TrialClassA oldValue = item.getValue();
                    if (oldValue == null) {
                        // System.out.println("oldValue was null so construct a dummy one");
                        oldValue = new TrialClassA();
                        item.setValue(oldValue);
                    }
                    oldValue.readExternal(in);
                    // System.out.println("read in value\n" + oldValue);
                } else {
                    throw new IllegalStateException("did not recognize your nullByteC: " + nullByteC);
                }


            } else {
                throw new IllegalStateException("did not recognize your nullByteA: " + nullByteA);
            }
        }
    }

    private static class Writer implements FastArray.WriteExternalFunction<KeyValuePairLongToObject<TrialClassA>> {

        @Override
        public void writeExternal(ObjectOutput out, KeyValuePairLongToObject<TrialClassA> item) throws IOException {
            if (item == null) {
                // System.out.println("write nullByteA = 1");
                out.writeByte(1); // nullByteA
            } else {
                // System.out.println("write nullByteA = 0");
                out.writeByte(0); // nullByteA

                // key
                long key = item.getKey();
                // System.out.println("write key: "+key);
                out.writeLong(key);

                // value
                TrialClassA value = item.getValue();
                if (value == null) {
                    // System.out.println("write nullByteC = 1");
                    out.writeByte(1); // nullByteC
                } else {
                    // System.out.println("write nullByteC = 0");
                    out.writeByte(0); // nullByteC

                    // System.out.println("write vlue:\n" + value);
                    value.writeExternal(out);
                }
            }
        }
    }

    static final Writer writer = new Writer();
    static final Reader reader = new Reader();

    public void testSimple() {
        long key = 123L;
        double double1 = 324234.897;
        int int1 = 436;
        long long1 = 978234897L;
        TrialClassA value = new TrialClassA(double1, int1, long1);

        KeyValuePairLongToObject<TrialClassA> kvp = new KeyValuePairLongToObject<TrialClassA>(key, value);
        // values in key
        assertEquals(key, kvp.getKey());

        // values in value
        assertEquals(double1, kvp.getValue().getDouble1());
        assertEquals(int1, kvp.getValue().getInt1());
        assertEquals(long1, kvp.getValue().getLong1());
    }

    public void testCloneAndEquals() {
        Random myRandom = new Random(89324L);
        KeyValuePairLongToObject<TrialClassA> kvp1 = makeRandomKvp(myRandom);
        KeyValuePairLongToObject<TrialClassA> kvp2 = makeRandomKvp(myRandom);
        assertFalse(kvp1.equals(kvp2));
        assertFalse(kvp2.equals(kvp1));

        // copy it over
        kvp2 = kvp1.safeClone();
        assertTrue(kvp1.equals(kvp2));
        assertTrue(kvp2.equals(kvp1));

        // put some other stuff in
        kvp1.setValue(FastArrayTest.makeRandomTestObject(myRandom));
        assertFalse(kvp1.equals(kvp2));
        assertFalse(kvp2.equals(kvp1));

        // copy it over again
        kvp1 = kvp2.safeClone();
        assertTrue(kvp1.equals(kvp2));
        assertTrue(kvp2.equals(kvp1));
    }

    public void testCopyValuesDeep() {
        Random myRandom = new Random(89324L);
        KeyValuePairLongToObject<TrialClassA> kvp1 = makeRandomKvp(myRandom);
        KeyValuePairLongToObject<TrialClassA> kvp2 = makeRandomKvp(myRandom);
        assertFalse(kvp1.equals(kvp2));
        assertFalse(kvp2.equals(kvp1));

        // copy it over
        kvp2.copyValues(kvp1);
        assertTrue(kvp1.equals(kvp2));
        assertTrue(kvp2.equals(kvp1));

        // put some other stuff in
        kvp1.setValue(FastArrayTest.makeRandomTestObject(myRandom));
        assertFalse(kvp1.equals(kvp2));
        assertFalse(kvp2.equals(kvp1));

        // copy it over again
        kvp1.copyValues(kvp2);
        assertTrue(kvp1.equals(kvp2));
        assertTrue(kvp2.equals(kvp1));
    }

    public void checkExternalization(KeyValuePairLongToObject<TrialClassA> kvpInput,
            KeyValuePairLongToObject<TrialClassA> kvpReceiver) throws Exception {
        if (kvpInput == null) {
            fail("writing from a null kvpInput");
        }
        if (kvpReceiver == null) {
            fail("reading to a null kvpInput");
        }


        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        writer.writeExternal(oos, kvpInput);
        oos.close();

        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bis);
        reader.readExternal(ois, kvpReceiver);
        if (kvpInput == null) {
            assertTrue(kvpReceiver == null);
        } else {
            // System.out.println("kvpInput:\n" + kvpInput);
            // System.out.println("kvpReceiver:\n" + kvpReceiver);
            assertTrue(kvpInput.equals(kvpReceiver));
            assertTrue(kvpReceiver.equals(kvpInput));
        }
    }

    public void testExternalizationNullInputsNullReceiver() throws Exception {
        KeyValuePairLongToObject<TrialClassA> kvpInput = new KeyValuePairLongToObject<TrialClassA>();
        KeyValuePairLongToObject<TrialClassA> kvpReceiver = new KeyValuePairLongToObject<TrialClassA>();
        checkExternalization(kvpInput, kvpReceiver);
    }

    public void testExternalizationNullInputsValidReceiver() throws Exception {
        Random myRandom = new Random(89324L);
        KeyValuePairLongToObject<TrialClassA> kvpInput = new KeyValuePairLongToObject<TrialClassA>();
        KeyValuePairLongToObject<TrialClassA> kvpReceiver = makeRandomKvp(myRandom);
        checkExternalization(kvpInput, kvpReceiver);
    }

    public void testExternalizationValidInputsNullReceiver() throws Exception {
        Random myRandom = new Random(89324L);
        KeyValuePairLongToObject<TrialClassA> kvpReceiver = new KeyValuePairLongToObject<TrialClassA>();
        KeyValuePairLongToObject<TrialClassA> kvpInput = makeRandomKvp(myRandom);
        checkExternalization(kvpInput, kvpReceiver);
    }

    public void testExternalizationValidInputsValidReceiver() throws Exception {
        Random myRandom = new Random(89324L);
        KeyValuePairLongToObject<TrialClassA> kvpInput = makeRandomKvp(myRandom);
        KeyValuePairLongToObject<TrialClassA> kvpReceiver = makeRandomKvp(myRandom);
        checkExternalization(kvpInput, kvpReceiver);
    }
}
