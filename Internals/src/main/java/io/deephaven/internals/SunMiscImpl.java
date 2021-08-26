package io.deephaven.internals;

import java.lang.reflect.Field;

class SunMiscImpl implements JdkInternals {

    SunMiscImpl() {}

    @Override
    public Unsafe getUnsafe() {
        return UnsafeImpl.INSTANCE;
    }

    @Override
    public DirectMemoryStats getDirectMemoryStats() {
        return DirectMemoryStatsImpl.INSTANCE;
    }

    enum UnsafeImpl implements Unsafe {
        INSTANCE;

        private static final sun.misc.Unsafe unsafe;
        static {
            try {
                Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = (sun.misc.Unsafe) field.get(null);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long allocateMemory(long size) {
            return unsafe.allocateMemory(size);
        }

        @Override
        public void freeMemory(long address) {
            unsafe.freeMemory(address);
        }

        @Override
        public long reallocateMemory(long address, long newSize) {
            return unsafe.reallocateMemory(address, newSize);
        }

        @Override
        public byte getByte(long address) {
            return unsafe.getByte(address);
        }

        @Override
        public void putByte(long address, byte value) {
            unsafe.putByte(address, value);
        }

        @Override
        public short getShort(long address) {
            return unsafe.getShort(address);
        }

        @Override
        public void putShort(long address, short value) {
            unsafe.putShort(address, value);
        }

        @Override
        public char getChar(long address) {
            return unsafe.getChar(address);
        }

        @Override
        public void putChar(long address, char value) {
            unsafe.putChar(address, value);
        }

        @Override
        public int getInt(long address) {
            return unsafe.getInt(address);
        }

        @Override
        public void putInt(long address, int value) {
            unsafe.putInt(address, value);
        }

        @Override
        public long getLong(long address) {
            return unsafe.getLong(address);
        }

        @Override
        public void putLong(long address, long value) {
            unsafe.putLong(address, value);
        }

        @Override
        public float getFloat(long address) {
            return unsafe.getFloat(address);
        }

        @Override
        public void putFloat(long address, float value) {
            unsafe.putFloat(address, value);
        }

        @Override
        public double getDouble(long address) {
            return unsafe.getDouble(address);
        }

        @Override
        public void putDouble(long address, double value) {
            unsafe.putDouble(address, value);
        }
    }

    enum DirectMemoryStatsImpl implements DirectMemoryStats {
        INSTANCE;

        @Override
        public long maxDirectMemory() {
            return sun.misc.VM.maxDirectMemory();
        }

        @Override
        public long getMemoryUsed() {
            return sun.misc.SharedSecrets.getJavaNioAccess().getDirectBufferPool().getMemoryUsed();
        }
    }
}
