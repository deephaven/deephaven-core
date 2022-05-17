package io.deephaven.parquet.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NonClosingInputStream extends InputStream {
    private final InputStream wrapped;

    public NonClosingInputStream(InputStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public int read() throws IOException {
        return wrapped.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return wrapped.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return wrapped.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return wrapped.readAllBytes();
    }

    @Override
    public byte[] readNBytes(int len) throws IOException {
        return wrapped.readNBytes(len);
    }

    @Override
    public int readNBytes(byte[] b, int off, int len) throws IOException {
        return wrapped.readNBytes(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return wrapped.skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapped.available();
    }

    @Override
    public void close() throws IOException {
        // no-op
    }

    @Override
    public void mark(int readlimit) {
        wrapped.mark(readlimit);
    }

    @Override
    public void reset() throws IOException {
        wrapped.reset();
    }

    @Override
    public boolean markSupported() {
        return wrapped.markSupported();
    }

    @Override
    public long transferTo(OutputStream out) throws IOException {
        return wrapped.transferTo(out);
    }
}
