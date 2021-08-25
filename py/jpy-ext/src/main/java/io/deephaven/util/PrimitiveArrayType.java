package io.deephaven.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface PrimitiveArrayType<T> extends ArrayType<T> {

    class Constants {
        private static final Map<Class<?>, PrimitiveArrayType<?>> primitiveTypeMap;
        private static final Map<Class<?>, PrimitiveArrayType<?>> arrayTypeMap;

        static {
            primitiveTypeMap = new HashMap<>();
            arrayTypeMap = new HashMap<>();

            for (PrimitiveArrayType<?> po : types()) {
                if (primitiveTypeMap.put(po.getPrimitiveType(), po) != null) {
                    throw new IllegalStateException();
                }
                if (arrayTypeMap.put(po.getArrayType(), po) != null) {
                    throw new IllegalStateException();
                }
            }
        }
    }

    static Optional<PrimitiveArrayType<?>> lookupForPrimitiveType(Class<?> clazz) {
        return Optional.ofNullable(Constants.primitiveTypeMap.get(clazz));
    }

    static Optional<PrimitiveArrayType<?>> lookupForArrayType(Class<?> clazz) {
        return Optional.ofNullable(Constants.arrayTypeMap.get(clazz));
    }

    static Optional<PrimitiveArrayType<?>> lookupForObject(Object array) {
        return lookupForArrayType(array.getClass());
    }

    static List<PrimitiveArrayType<?>> types() {
        return Arrays.asList(
            booleans(),
            bytes(),
            chars(),
            shorts(),
            ints(),
            longs(),
            floats(),
            doubles());
    }

    static Booleans booleans() {
        return Booleans.INSTANCE;
    }

    static Bytes bytes() {
        return Bytes.INSTANCE;
    }

    static Chars chars() {
        return Chars.INSTANCE;
    }

    static Shorts shorts() {
        return Shorts.INSTANCE;
    }

    static Ints ints() {
        return Ints.INSTANCE;
    }

    static Longs longs() {
        return Longs.INSTANCE;
    }

    static Floats floats() {
        return Floats.INSTANCE;
    }

    static Doubles doubles() {
        return Doubles.INSTANCE;
    }

    Class<?> getPrimitiveType();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Booleans booleans);

        void visit(Bytes bytes);

        void visit(Chars chars);

        void visit(Shorts shorts);

        void visit(Ints ints);

        void visit(Longs longs);

        void visit(Floats floats);

        void visit(Doubles doubles);
    }

    enum Booleans implements PrimitiveArrayType<boolean[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return boolean.class;
        }

        @Override
        public Class<boolean[]> getArrayType() {
            return boolean[].class;
        }

        @Override
        public int getLength(boolean[] array) {
            return array.length;
        }

        @Override
        public boolean[] newInstance(int len) {
            return new boolean[len];
        }

        @Override
        public void arrayCopy(boolean[] src, int srcPos, boolean[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Bytes implements PrimitiveArrayType<byte[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return byte.class;
        }

        @Override
        public Class<byte[]> getArrayType() {
            return byte[].class;
        }

        @Override
        public int getLength(byte[] array) {
            return array.length;
        }

        @Override
        public byte[] newInstance(int len) {
            return new byte[len];
        }

        @Override
        public void arrayCopy(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Chars implements PrimitiveArrayType<char[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return char.class;
        }

        @Override
        public Class<char[]> getArrayType() {
            return char[].class;
        }

        @Override
        public int getLength(char[] array) {
            return array.length;
        }

        @Override
        public char[] newInstance(int len) {
            return new char[len];
        }

        @Override
        public void arrayCopy(char[] src, int srcPos, char[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Shorts implements PrimitiveArrayType<short[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return short.class;
        }

        @Override
        public Class<short[]> getArrayType() {
            return short[].class;
        }

        @Override
        public int getLength(short[] array) {
            return array.length;
        }

        @Override
        public short[] newInstance(int len) {
            return new short[len];
        }

        @Override
        public void arrayCopy(short[] src, int srcPos, short[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Ints implements PrimitiveArrayType<int[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return int.class;
        }

        @Override
        public Class<int[]> getArrayType() {
            return int[].class;
        }

        @Override
        public int getLength(int[] array) {
            return array.length;
        }

        @Override
        public int[] newInstance(int len) {
            return new int[len];
        }

        @Override
        public void arrayCopy(int[] src, int srcPos, int[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Longs implements PrimitiveArrayType<long[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return long.class;
        }

        @Override
        public Class<long[]> getArrayType() {
            return long[].class;
        }

        @Override
        public int getLength(long[] array) {
            return array.length;
        }

        @Override
        public long[] newInstance(int len) {
            return new long[len];
        }

        @Override
        public void arrayCopy(long[] src, int srcPos, long[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Floats implements PrimitiveArrayType<float[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return float.class;
        }

        @Override
        public Class<float[]> getArrayType() {
            return float[].class;
        }

        @Override
        public int getLength(float[] array) {
            return array.length;
        }

        @Override
        public float[] newInstance(int len) {
            return new float[len];
        }

        @Override
        public void arrayCopy(float[] src, int srcPos, float[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }

    enum Doubles implements PrimitiveArrayType<double[]> {
        INSTANCE;

        @Override
        public Class<?> getPrimitiveType() {
            return double.class;
        }

        @Override
        public Class<double[]> getArrayType() {
            return double[].class;
        }

        @Override
        public int getLength(double[] array) {
            return array.length;
        }

        @Override
        public double[] newInstance(int len) {
            return new double[len];
        }

        @Override
        public void arrayCopy(double[] src, int srcPos, double[] dest, int destPos, int length) {
            System.arraycopy(src, srcPos, dest, destPos, length);
        }

        @Override
        public <V extends Visitor> V walk(V visitor) {
            visitor.visit(this);
            return visitor;
        }
    }
}
