/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.io.*;
import java.util.zip.*;

public class ZipInputStream extends FilterInputStream {
    final public static int BUFFER_LIMIT = 64 * 1024 * 1024; // 64MB

    protected DataInputStream dataIn; // InputStream being read
    protected InflaterInputStream iis; // Inflater on compressed data

    public ZipInputStream(InputStream in) throws IOException {
        super(in);
        dataIn = new DataInputStream(in);
        fill();
    }

    private void fill() throws IOException {
        byte buf[];
        if (iis != null) {
            iis.close();
            iis = null;
        }
        int noBytes;
        try {
            noBytes = dataIn.readInt();
            if (noBytes < 0 || noBytes > BUFFER_LIMIT)
                throw new ZipException("io.deephaven.io.streams.ZipInputStream was asked to read " + noBytes
                        + " which exceeded buffer limit.  Is this a Zip file?");
            buf = new byte[noBytes];
        } catch (EOFException ee) {
            return;
        }
        dataIn.readFully(buf);
        iis = new InflaterInputStream(new ByteArrayInputStream(buf));
    }

    public synchronized int read() throws IOException {
        int ret = iis.read();

        while (ret == -1) {
            fill();
            if (iis == null) {
                // there is no more compressed data
                break;
            }
            // decompress first byte (returns -1 if writer wrote
            // chunk of compressed data of size 0)
            ret = iis.read();
        }
        return ret;
    }

    public synchronized int read(byte b[])
            throws IOException {
        return read(b, 0, b.length);
    }

    public synchronized int read(byte b[], int off, int len)
            throws IOException {
        if (iis.available() == 0) {
            fill();
            if (iis == null) {
                // there is no more compressed data
                return -1;
            }
        }
        int i = 0;
        for (i = 0; i < len; i++) {
            int data = iis.read();
            if (data == -1)
                break;
            b[i + off] = (byte) data;
        }
        return i;
    }

    public synchronized long skip(long n) throws IOException {
        ensureOpen();
        for (int i = 0; i < n; i++)
            read();
        return n;
    }

    // Returns non-zero if more data available.
    public synchronized int available() throws IOException {
        return in.available() + iis.available();
    }

    public boolean markSupported() {
        return false;
    }

    public void close() throws IOException {
        if (in == null)
            return;
        in.close();
        in = null;
    }

    private void ensureOpen() throws IOException {
        if (in == null)
            throw new IOException("Stream closed");
    }

}
