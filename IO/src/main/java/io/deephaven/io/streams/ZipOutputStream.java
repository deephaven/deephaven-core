/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.io.*;
import java.util.zip.*;

public class ZipOutputStream extends FilterOutputStream {
    protected byte buf[]; // The internal buffer where uncompressed data is stored
    protected int count; // The number of valid bytes in the buffer
    protected int size; // Total size of the buffer.

    public ZipOutputStream(OutputStream out) {
        this(out, 512);
    }

    public ZipOutputStream(OutputStream out, int size) {
        super(out);
        this.size = size;
        buf = new byte[size];
    }

    private void flushBuffer() throws IOException {
        if (count > 0) {
            writeBuffer(buf, 0, count);
            count = 0;
        }
    }

    private void writeBuffer(byte buf[], int offset, int len) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len / 2 + 1);
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater);
        dos.write(buf, offset, len);
        dos.finish();
        dos.flush();

        DataOutputStream dataOut = new DataOutputStream(out);
        byte[] nbuf = baos.toByteArray();
        dataOut.writeInt(nbuf.length); // write number of data bytes
        dataOut.write(nbuf, 0, nbuf.length);
        dataOut.flush();

        buf = new byte[size];
    }

    public synchronized void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte) b;
    }

    public synchronized void write(byte b[]) throws IOException {
        write(b, 0, b.length);
    }

    public synchronized void write(byte b[], int off, int len) throws IOException {
        if (len >= buf.length) {
            /*
             * If the request length exceeds the size of the output buffer, flush the output buffer and then write the
             * data directly. In this way buffered streams will cascade harmlessly.
             */
            flushBuffer();
            writeBuffer(b, off, len);
            return;
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    public synchronized void flush() throws IOException {
        flushBuffer();
        out.flush();
    }

}
