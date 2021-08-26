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

public class KeyValuePairTest extends TestCase {

    private static KeyValuePair<LongWrapper, TrialClassA> makeRandomKvp(Random myRandom) {
        LongWrapper key = new LongWrapper(myRandom.nextLong());
        TrialClassA value = FastArrayTest.makeRandomTestObject(myRandom);
        return new KeyValuePair<LongWrapper, TrialClassA>(key, value);
    }

    private static class Reader implements FastArray.ReadExternalFunction<KeyValuePair<LongWrapper, TrialClassA>> {

        @Override
        public void readExternal(ObjectInput in, KeyValuePair<LongWrapper, TrialClassA> item)
                throws IOException, ClassNotFoundException {
            byte nullByteA = in.readByte();
            // System.out.println("read nullByteA: " + nullByteA);
            if (nullByteA == 1) {
                // System.out.println("item is null");
                item = null;
            } else if (nullByteA == 0) {

                // key
                byte nullByteB = in.readByte();
                // System.out.println("read nullByteB: " + nullByteB);
                if (nullByteB == 1) {
                    // System.out.println("item.key is null");
                    item.setKey(null);
                } else if (nullByteB == 0) {
                    LongWrapper oldKey = item.getKey();
                    if (oldKey == null) {
                        // System.out.println("oldKey was null so construct a dummy one");
                        oldKey = new LongWrapper();
                        item.setKey(oldKey);
                    }
                    oldKey.readExternal(in);
                    // System.out.println("read in key\n" + oldKey);
                } else {
                    throw new IllegalStateException("did not recognize your nullByteB: " + nullByteB);
                }

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

    private static class Writer implements FastArray.WriteExternalFunction<KeyValuePair<LongWrapper, TrialClassA>> {

        @Override
        public void writeExternal(ObjectOutput out, KeyValuePair<LongWrapper, TrialClassA> item) throws IOException {
            if (item == null) {
                // System.out.println("write nullByteA = 1");
                out.writeByte(1); // nullByteA
            } else {
                // System.out.println("write nullByteA = 0");
                out.writeByte(0); // nullByteA

                // key
                LongWrapper key = item.getKey();
                if (key == null) {
                    // System.out.println("write nullByteB = 1");
                    out.writeByte(1); // nullByteB
                } else {
                    // System.out.println("write nullByteB = 0");
                    out.writeByte(0); // nullByteB

                    // System.out.println("write key\n" + key);
                    key.writeExternal(out);
                }

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
        long longKey = 123L;
        LongWrapper key = new LongWrapper(longKey);
        double double1 = 324234.897;
        int int1 = 436;
        long long1 = 978234897L;
        TrialClassA value = new TrialClassA(double1, int1, long1);

        KeyValuePair<LongWrapper, TrialClassA> kvp = new KeyValuePair<LongWrapper, TrialClassA>(key, value);
        // values in key
        assertEquals(longKey, kvp.getKey().getVal());

        // values in value
        assertEquals(double1, kvp.getValue().getDouble1());
        assertEquals(int1, kvp.getValue().getInt1());
        assertEquals(long1, kvp.getValue().getLong1());
    }

    public void testCloneAndEquals() {
        Random myRandom = new Random(89324L);
        KeyValuePair<LongWrapper, TrialClassA> kvp1 = makeRandomKvp(myRandom);
        KeyValuePair<LongWrapper, TrialClassA> kvp2 = makeRandomKvp(myRandom);
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
        KeyValuePair<LongWrapper, TrialClassA> kvp1 = makeRandomKvp(myRandom);
        KeyValuePair<LongWrapper, TrialClassA> kvp2 = makeRandomKvp(myRandom);
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

    public void checkExternalization(KeyValuePair<LongWrapper, TrialClassA> kvpInput,
            KeyValuePair<LongWrapper, TrialClassA> kvpReceiver) throws Exception {
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
        KeyValuePair<LongWrapper, TrialClassA> kvpInput = new KeyValuePair<LongWrapper, TrialClassA>();
        KeyValuePair<LongWrapper, TrialClassA> kvpReceiver = new KeyValuePair<LongWrapper, TrialClassA>();
        checkExternalization(kvpInput, kvpReceiver);
    }

    public void testExternalizationNullInputsValidReceiver() throws Exception {
        Random myRandom = new Random(89324L);
        KeyValuePair<LongWrapper, TrialClassA> kvpInput = new KeyValuePair<LongWrapper, TrialClassA>();
        KeyValuePair<LongWrapper, TrialClassA> kvpReceiver = makeRandomKvp(myRandom);
        checkExternalization(kvpInput, kvpReceiver);
    }

    public void testExternalizationValidInputsNullReceiver() throws Exception {
        Random myRandom = new Random(89324L);
        KeyValuePair<LongWrapper, TrialClassA> kvpReceiver = new KeyValuePair<LongWrapper, TrialClassA>();
        KeyValuePair<LongWrapper, TrialClassA> kvpInput = makeRandomKvp(myRandom);
        checkExternalization(kvpInput, kvpReceiver);
    }

    public void testExternalizationValidInputsValidReceiver() throws Exception {
        Random myRandom = new Random(89324L);
        KeyValuePair<LongWrapper, TrialClassA> kvpInput = makeRandomKvp(myRandom);
        KeyValuePair<LongWrapper, TrialClassA> kvpReceiver = makeRandomKvp(myRandom);
        checkExternalization(kvpInput, kvpReceiver);
    }
}
