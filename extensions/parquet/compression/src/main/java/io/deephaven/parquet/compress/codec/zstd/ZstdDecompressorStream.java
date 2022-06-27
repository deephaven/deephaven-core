package io.deephaven.parquet.compress.codec.zstd;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import org.apache.hadoop.io.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

public class ZstdDecompressorStream extends CompressionInputStream {

    private final ZstdInputStreamNoFinalizer zstdInputStream;

    public ZstdDecompressorStream(InputStream stream) throws IOException {
        super(stream);
        zstdInputStream = new ZstdInputStreamNoFinalizer(stream);
    }

    public ZstdDecompressorStream(InputStream stream, BufferPool pool) throws IOException {
        super(stream);
        zstdInputStream = new ZstdInputStreamNoFinalizer(stream, pool);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        return zstdInputStream.read(b, off, len);
    }

    public int read() throws IOException {
        return zstdInputStream.read();
    }

    public void resetState() throws IOException {
        // no-opt, doesn't apply to ZSTD
    }

    @Override
    public void close() throws IOException {
        try {
            zstdInputStream.close();
        } finally {
            super.close();
        }
    }
}
