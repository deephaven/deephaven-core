package io.deephaven.parquet.compress;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface Compressor {
    Compressor NOOP = new Compressor() {
        @Override
        public OutputStream compress(OutputStream os) throws IOException {
            return os;
        }

        @Override
        public InputStream decompress(InputStream is) throws IOException {
            return is;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize) {
            return BytesInput.from(inputStream, compressedSize);
        }
    };


    OutputStream compress(OutputStream os) throws IOException;

    default OutputStream compressNoClose(OutputStream os) throws IOException {
        return compress(new NonClosingOutputStream(os));
    }

    InputStream decompress(InputStream is) throws IOException;

    default InputStream decompressNoClose(InputStream is) throws IOException {
        return decompress(new NonClosingInputStream(is));
    }

    CompressionCodecName getCodecName();

    BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize) throws IOException;
}
