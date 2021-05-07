/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.dataobjects.AbstractDataObject;

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

    public static FieldToArray getFieldToArrayBufferAdoSingleDay(Class type, int columnIndex, int bufferSize) {
        if (type == byte.class || type == Byte.class) {
            return new ByteAdoIndexFieldToArray(new byte[bufferSize], columnIndex);
        } else if (type == double.class || type == Double.class) {
            return new DoubleAdoIndexFieldToArray(new double[bufferSize], columnIndex);
        } else if (type == int.class || type == Integer.class) {
            return new IntAdoIndexFieldToArray(new int[bufferSize], columnIndex);
        } else if (type == long.class || type == Long.class || type == Date.class) {
            return new LongAdoIndexFieldToArray(new long[bufferSize], columnIndex);
        } else {
            return new ObjectAdoIndexFieldToArray(new Object[bufferSize], columnIndex);
        }
    }


    public static FieldToArray getFieldToArrayBufferAdoMultiDay(Class type, String columnName, int bufferSize) {
        if (type == byte.class || type == Byte.class) {
            return new ByteAdoNameFieldToArray(new byte[bufferSize], columnName);
        } else if (type == double.class || type == Double.class) {
            return new DoubleAdoNameFieldToArray(new double[bufferSize], columnName);
        } else if (type == int.class || type == Integer.class) {
            return new IntAdoNameFieldToArray(new int[bufferSize], columnName);
        } else if (type == long.class || type == Long.class || type == Date.class) {
            return new LongAdoNameFieldToArray(new long[bufferSize], columnName);
        } else {
            return new ObjectAdoNameFieldToArray(new Object[bufferSize], columnName);
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------------------------------------

    static interface FieldToArray {
        void setValue(Object obj, int position);

        Object getArray();
    }

    //------------------------------------------------------------------------------------------------------------------------------------------------------------------

    static class ByteAdoNameFieldToArray implements FieldToArray {
        public byte[] buffer;
        private String columnName;

        ByteAdoNameFieldToArray(byte[] buffer, String columnName) {
            this.buffer = buffer;
            this.columnName = columnName;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getByte(columnName);
        }

        @Override
        public Object getArray() {
            return buffer;
        }
    }

    static class DoubleAdoNameFieldToArray implements FieldToArray {
        public double[] buffer;
        private String columnName;

        @Override
        public Object getArray() {
            return buffer;
        }

        DoubleAdoNameFieldToArray(double[] buffer, String columnName) {
            this.buffer = buffer;
            this.columnName = columnName;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getDouble(columnName);
        }
    }

    static class IntAdoNameFieldToArray implements FieldToArray {
        public int[] buffer;
        private String columnName;

        @Override
        public Object getArray() {
            return buffer;
        }

        IntAdoNameFieldToArray(int[] buffer, String columnName) {
            this.buffer = buffer;
            this.columnName = columnName;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getInt(columnName);
        }
    }

    static class LongAdoNameFieldToArray implements FieldToArray {
        public long[] buffer;
        private String columnName;

        @Override
        public Object getArray() {
            return buffer;
        }

        LongAdoNameFieldToArray(long[] buffer, String columnName) {
            this.buffer = buffer;
            this.columnName = columnName;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getLong(columnName);
        }
    }

    static class ObjectAdoNameFieldToArray implements FieldToArray {
        public Object[] buffer;
        private String columnName;

        ObjectAdoNameFieldToArray(Object[] buffer, String columnName) {
            this.buffer = buffer;
            this.columnName = columnName;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getValue(columnName);
        }

        @Override
        public Object getArray() {
            return buffer;
        }
    }

    //------------------------------------------------------------------------------------------------------------------------------------------------------------------

    static class ByteAdoIndexFieldToArray implements FieldToArray {
        public byte[] buffer;
        private int columnIndex;

        ByteAdoIndexFieldToArray(byte[] buffer, int columnIndex) {
            this.buffer = buffer;
            this.columnIndex = columnIndex;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getByte(columnIndex);
        }

        @Override
        public Object getArray() {
            return buffer;
        }
    }

    static class DoubleAdoIndexFieldToArray implements FieldToArray {
        public double[] buffer;
        private int columnIndex;

        @Override
        public Object getArray() {
            return buffer;
        }

        DoubleAdoIndexFieldToArray(double[] buffer, int columnIndex) {
            this.buffer = buffer;
            this.columnIndex = columnIndex;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getDouble(columnIndex);
        }
    }

    static class IntAdoIndexFieldToArray implements FieldToArray {
        public int[] buffer;
        private int columnIndex;

        @Override
        public Object getArray() {
            return buffer;
        }

        IntAdoIndexFieldToArray(int[] buffer, int columnIndex) {
            this.buffer = buffer;
            this.columnIndex = columnIndex;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getInt(columnIndex);
        }
    }

    static class LongAdoIndexFieldToArray implements FieldToArray {
        public long[] buffer;
        private int columnIndex;

        @Override
        public Object getArray() {
            return buffer;
        }

        LongAdoIndexFieldToArray(long[] buffer, int columnIndex) {
            this.buffer = buffer;
            this.columnIndex = columnIndex;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getLong(columnIndex);
        }
    }

    static class ObjectAdoIndexFieldToArray implements FieldToArray {
        public Object[] buffer;
        private int columnIndex;

        ObjectAdoIndexFieldToArray(Object[] buffer, int columnIndex) {
            this.buffer = buffer;
            this.columnIndex = columnIndex;
        }

        public void setValue(Object obj, int position) {
            buffer[position] = ((AbstractDataObject) obj).getValue(columnIndex);
        }

        @Override
        public Object getArray() {
            return buffer;
        }
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
