/*
 * Copyright (c) 2016-2021 Deephaven and Patent Pending
 */

package io.deephaven.base.array;

import io.deephaven.base.stats.State;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;

import java.security.AccessController;
import java.security.PrivilegedAction;
import io.deephaven.internals.JdkInternalsLoader;
import io.deephaven.internals.Unsafe;

/**
 * Wrapper for Unsafe operations, using reflection to operate on the appropriate class.
 */
public class PrimitiveReaderImpl implements PrimitiveReader {
    private static final Unsafe unsafe = AccessController.doPrivileged(
        (PrivilegedAction<Unsafe>) () -> JdkInternalsLoader.getInstance().getUnsafe());

    private long address;
    private long offset;
    private long length;
    private long allocated;

    private static final Value reallocatedSize =
        Stats.makeItem("PrimitiveReaderImpl", "reallocatedSize", State.FACTORY).getValue();

    public PrimitiveReaderImpl(int bytes) {
        this.allocated = bytes;
        this.offset = 0;
        this.length = 0;

        address = unsafe.allocateMemory(allocated);
        if (address == 0) {
            throw new OutOfMemoryError(
                "Can't allocate unsafe memory... tried to allocate=" + allocated);
        }
    }

    public void reset() {
        offset = 0;
        length = 0;
    }

    public void free() {
        unsafe.freeMemory(address);
    }

    private void ensureRemaining(int bytesRemaining) {
        if (offset + bytesRemaining > allocated) {
            final long newSize = Math.max(offset + bytesRemaining, allocated * 2);
            final long newAddress = unsafe.reallocateMemory(address, newSize);
            if (newAddress == 0) {
                throw new OutOfMemoryError("Not enough memore for reallocate memory! currentSize="
                    + allocated + ", newSize=" + newSize);
            }
            address = newAddress;
            allocated = newSize;
            reallocatedSize.sample(newSize);
        }
    }

    @Override
    public int remaining() {
        return (int) (length - offset);
    }

    public void put(boolean b) {
        ensureRemaining(1);
        unsafe.putByte(address + (offset += 1) - 1, b ? (byte) 1 : (byte) 0);
    }

    public void put(byte b) {
        ensureRemaining(1);
        unsafe.putByte(address + (offset += 1) - 1, b);
    }

    public void put(char c) {
        ensureRemaining(2);
        unsafe.putChar(address + (offset += 2) - 2, c);
    }

    public void put(short c) {
        ensureRemaining(2);
        unsafe.putShort(address + (offset += 2) - 2, c);
    }

    public void put(int c) {
        ensureRemaining(4);
        unsafe.putInt(address + (offset += 4) - 4, c);
    }

    public void put(long c) {
        ensureRemaining(8);
        unsafe.putLong(address + (offset += 8) - 8, c);
    }

    public void put(float c) {
        ensureRemaining(5);
        unsafe.putFloat(address + (offset += 4) - 4, c);
    }

    public void put(double c) {
        ensureRemaining(8);
        unsafe.putDouble(address + (offset += 8) - 8, c);
    }

    public void putSkip(int bytes) {
        ensureRemaining(bytes);
        offset += bytes;
    }

    public void doneAdding() {
        length = offset;
        offset = 0;
    }

    @Override
    public boolean nextBoolean() {
        return unsafe.getByte(address + (offset += 1) - 1) != 0;
    }

    @Override
    public byte nextByte() {
        return unsafe.getByte(address + (offset += 1) - 1);
    }

    @Override
    public char nextChar() {
        return unsafe.getChar(address + (offset += 2) - 2);
    }

    @Override
    public short nextShort() {
        return unsafe.getShort(address + (offset += 2) - 2);
    }

    @Override
    public int nextInt() {
        return unsafe.getInt(address + (offset += 4) - 4);
    }

    @Override
    public long nextLong() {
        return unsafe.getLong(address + (offset += 8) - 8);
    }

    @Override
    public float nextFloat() {
        return unsafe.getFloat(address + (offset += 4) - 4);
    }

    @Override
    public double nextDouble() {
        return unsafe.getDouble(address + (offset += 8) - 8);
    }
}
