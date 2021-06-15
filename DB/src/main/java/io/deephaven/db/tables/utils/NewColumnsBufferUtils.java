/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import java.util.Date;

public class NewColumnsBufferUtils {

    public static FieldToArray getFieldToArrayBuffer(Class type, int bufferSize) {
        if (type == byte.class || type == Byte.class) {
            return new ByteFieldToArray(new byte[bufferSize]);
        } else if (type == double.class || type == Double.class) {
            return new DoubleFieldToArray(new double[bufferSize]);
        } else if (type == int.class || type == Integer.class) {
            return new IntFieldToArray(new int[bufferSize]);
        } else if (type == long.class || type == Long.class || type == Date.class) {
            return new LongFieldToArray(new long[bufferSize]);
        } else {
            return new ObjectFieldToArray(new Object[bufferSize]);
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------------------------------------

    static interface FieldToArray {
        void setValue(Object obj, int position);

        Object getArray();
    }

    //------------------------------------------------------------------------------------------------------------------------------------------------------------------

    static class ByteFieldToArray implements FieldToArray {
        public byte[] buffer;

        ByteFieldToArray(byte[] buffer) {
            this.buffer = buffer;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = (Byte) obj;
        }

        @Override
        public Object getArray() {
            return buffer;
        }
    }

    static class DoubleFieldToArray implements FieldToArray {
        public double[] buffer;

        @Override
        public Object getArray() {
            return buffer;
        }

        DoubleFieldToArray(double[] buffer) {
            this.buffer = buffer;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = (Double) obj;
        }
    }

    static class IntFieldToArray implements FieldToArray {
        public int[] buffer;

        @Override
        public Object getArray() {
            return buffer;
        }

        IntFieldToArray(int[] buffer) {
            this.buffer = buffer;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = (Integer) obj;
        }
    }

    static class LongFieldToArray implements FieldToArray {
        public long[] buffer;

        @Override
        public Object getArray() {
            return buffer;
        }

        LongFieldToArray(long[] buffer) {
            this.buffer = buffer;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = (Long) obj;
        }
    }

    static class ObjectFieldToArray implements FieldToArray {
        public Object[] buffer;

        @Override
        public Object getArray() {
            return buffer;
        }

        ObjectFieldToArray(Object[] buffer) {
            this.buffer = buffer;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = obj;
        }
    }
}
