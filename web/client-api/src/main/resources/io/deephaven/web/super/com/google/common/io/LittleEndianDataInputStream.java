/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.common.io;

import org.gwtproject.nio.Numbers;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * An implementation of {@link DataInput} that uses little-endian byte ordering for reading {@code
 * short}, {@code int}, {@code float}, {@code double}, and {@code long} values.
 *
 * <p><b>Note:</b> This class intentionally violates the specification of its supertype {@code
 * DataInput}, which explicitly requires big-endian byte order.
 *
 * @author Chris Nokleberg
 * @author Keith Bottner
 * @since 8.0
 */
public final class LittleEndianDataInputStream extends FilterInputStream implements DataInput {

    public LittleEndianDataInputStream(InputStream in) {
        super(Objects.requireNonNull(in));
    }

    /** This method will throw an {@link UnsupportedOperationException}. */
    @Override
    public String readLine() {
        throw new UnsupportedOperationException("readLine is not supported");
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        int total = 0;
        while (total < b.length) {
            int result = read(b, total, b.length - total);
            if (result == -1) {
                break;
            }
            total += result;
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        int total = 0;
        while (total < len) {
            int result = read(b, off + total, len - total);
            if (result == -1) {
                break;
            }
            total += result;
        }
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return (int) in.skip(n);
    }

    @Override
    public int readUnsignedByte() throws IOException {
        int b1 = in.read();
        if (0 > b1) {
            throw new EOFException();
        }

        return b1;
    }

    @Override
    public int readUnsignedShort() throws IOException {
        byte b1 = readAndCheckByte();
        byte b2 = readAndCheckByte();

        int result = b2;
        result = (result << 8) | (b1 & 0xFF);

        return result;
    }

    @Override
    public int readInt() throws IOException {
        byte b1 = readAndCheckByte();
        byte b2 = readAndCheckByte();
        byte b3 = readAndCheckByte();
        byte b4 = readAndCheckByte();

        int result = b4;
        result = (result << 8) | (b3 & 0xFF);
        result = (result << 8) | (b2 & 0xFF);
        result = (result << 8) | (b1 & 0xFF);

        return result;
    }

    @Override
    public long readLong() throws IOException {
        byte b1 = readAndCheckByte();
        byte b2 = readAndCheckByte();
        byte b3 = readAndCheckByte();
        byte b4 = readAndCheckByte();
        byte b5 = readAndCheckByte();
        byte b6 = readAndCheckByte();
        byte b7 = readAndCheckByte();
        byte b8 = readAndCheckByte();

        long result = b8;
        result = (result << 8) | (b7 & 0xFF);
        result = (result << 8) | (b6 & 0xFF);
        result = (result << 8) | (b5 & 0xFF);
        result = (result << 8) | (b4 & 0xFF);
        result = (result << 8) | (b3 & 0xFF);
        result = (result << 8) | (b2 & 0xFF);
        result = (result << 8) | (b1 & 0xFF);

        return result;
    }

    /**
     * Reads a {@code float} as specified by {@link DataInputStream#readFloat()}, except using
     * little-endian byte order.
     *
     * @return the next four bytes of the input stream, interpreted as a {@code float} in
     *     little-endian byte order
     * @throws IOException if an I/O error occurs
     */
    @Override
    public float readFloat() throws IOException {
        return Numbers.intBitsToFloat(readInt());
    }

    /**
     * Reads a {@code double} as specified by {@link DataInputStream#readDouble()}, except using
     * little-endian byte order.
     *
     * @return the next eight bytes of the input stream, interpreted as a {@code double} in
     *     little-endian byte order
     * @throws IOException if an I/O error occurs
     */
    @Override
    public double readDouble() throws IOException {
        return Numbers.longBitsToDouble(readLong());
    }

    @Override
    public String readUTF() throws IOException {
        throw new UnsupportedOperationException("readUTF");
    }

    @Override
    public short readShort() throws IOException {
        return (short) readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return (char) readUnsignedShort();
    }

    @Override
    public byte readByte() throws IOException {
        return (byte) readUnsignedByte();
    }

    @Override
    public boolean readBoolean() throws IOException {
        return readUnsignedByte() != 0;
    }

    private byte readAndCheckByte() throws IOException, EOFException {
        int b1 = in.read();

        if (-1 == b1) {
            throw new EOFException();
        }

        return (byte) b1;
    }
}
