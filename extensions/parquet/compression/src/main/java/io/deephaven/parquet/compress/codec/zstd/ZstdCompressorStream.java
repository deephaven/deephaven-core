package io.deephaven.parquet.compress.codec.zstd;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import org.apache.hadoop.io.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class ZstdCompressorStream extends CompressionOutputStream {

    private final ZstdOutputStreamNoFinalizer zstdOutputStream;

    public ZstdCompressorStream(OutputStream stream, int level, int workers) throws IOException {
        super(stream);
        zstdOutputStream = new ZstdOutputStreamNoFinalizer(stream, level);
        zstdOutputStream.setWorkers(workers);
    }

    public ZstdCompressorStream(OutputStream stream, BufferPool pool, int level, int workers) throws IOException {
        super(stream);
        zstdOutputStream = new ZstdOutputStreamNoFinalizer(stream, pool);
        zstdOutputStream.setLevel(level);
        zstdOutputStream.setWorkers(workers);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        zstdOutputStream.write(b, off, len);
    }

    public void write(int b) throws IOException {
        zstdOutputStream.write(b);
    }

    public void finish() throws IOException {
        // no-op, doesn't apply to ZSTD
    }

    public void resetState() throws IOException {
        // no-op, doesn't apply to ZSTD
    }

    @Override
    public void flush() throws IOException {
        zstdOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        zstdOutputStream.close();
    }
}
