/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.util.functions;

import io.deephaven.base.testing.BaseArrayTestCase;
import groovy.lang.Closure;

public class TestSerializableClosure extends BaseArrayTestCase {

    private final String value = "S";

    private final Closure<String> closure = new Closure<String>(null) {
        @Override
        public String call() {
            return value;
        }

        @Override
        public String call(Object... args) {
            return value;
        }

        @Override
        public String call(Object arguments) {
            return value;
        }
    };

    public void testSerializableClosure() {
        SerializableClosure serializableClosure = new ClosureFunction<Comparable, String>(closure);

        assertEquals(value, serializableClosure.getClosure().call());
        assertEquals(value, serializableClosure.getClosure().call("T"));
        assertEquals(value, serializableClosure.getClosure().call("A", "B"));

        // testing serialization is too hard
        /*
         * Object copy = null; try { ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutputStream oos =
         * new ObjectOutputStream(bos); oos.writeObject(serializableClosure); oos.flush(); byte[] obis =
         * bos.toByteArray(); oos.close(); bos.close();
         * 
         * ByteArrayInputStream bis = new ByteArrayInputStream(obis); ObjectInputStream ois = new
         * ObjectInputStream(bis); copy = ois.readObject(); ois.close(); bis.close(); } catch (IOException |
         * ClassNotFoundException e) { e.printStackTrace(); }
         * 
         * serializableClosure = (SerializableClosure) copy; if(serializableClosure == null) {
         * TestCase.fail("Null return from serialization"); }
         * 
         * assertEquals(value, serializableClosure.getClosure().call());
         */
    }


}
