package io.deephaven.internals;

public interface Unsafe {
    long allocateMemory(long size);

    void freeMemory(long address);

    long reallocateMemory(long address, long newSize);

    byte getByte(long address);

    void putByte(long address, byte value);

    short getShort(long address);

    void putShort(long address, short value);

    char getChar(long address);

    void putChar(long address, char value);

    int getInt(long address);

    void putInt(long address, int value);

    long getLong(long address);

    void putLong(long address, long value);

    float getFloat(long address);

    void putFloat(long address, float value);

    double getDouble(long address);

    void putDouble(long address, double value);
}
