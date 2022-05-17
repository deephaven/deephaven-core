package io.deephaven.parquet.compress;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Stream;

public class DeephavenCodecFactory {
    public static class CodecWrappingCompressor implements Compressor {
        private final CompressionCodec compressionCodec;

        private CodecWrappingCompressor(CompressionCodec compressionCodec) {
            this.compressionCodec = compressionCodec;
        }

        @Override
        public OutputStream compress(OutputStream os) throws IOException {
            return compressionCodec.createOutputStream(os);
        }
        @Override
        public InputStream decompress(InputStream is) throws IOException {
            return compressionCodec.createInputStream(is);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return Stream.of(CompressionCodecName.values())
                    .filter(codec -> compressionCodec.getDefaultExtension().equals(codec.getExtension()))
                    .findAny().get();
        }

        @Override
        public BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize) throws IOException {
            Decompressor decompressor = CodecPool.getDecompressor(compressionCodec);
            if (decompressor != null) {
                // It is permitted for a decompressor to be null, otherwise we want to reset() it to
                // be ready for a new stream.
                // Note that this strictly shouldn't be necessary, since returnDecompressor will reset
                // it as well, but this is the pattern copied from CodecFactory.decompress.
                decompressor.reset();
            }

            try {
                // Note that we don't close this, we assume the caller will close their input stream when ready,
                // and this won't need to be closed.
                InputStream buffered = IOUtils.buffer(inputStream, compressedSize);
                CompressionInputStream decompressed = compressionCodec.createInputStream(buffered, decompressor);
                return BytesInput.copy(BytesInput.from(decompressed, uncompressedSize));
            } finally {
                // Always return it, the pool will decide if it should be reused or not.
                // CodecFactory has no logic around only returning after successful streams,
                // and the instance appears to leak otherwise.
                CodecPool.returnDecompressor(decompressor);
            }
        }
    }

    private static Configuration configurationWithCodecClasses(List<Class<?>> codecClasses) {
        Configuration conf = new Configuration();
        //noinspection unchecked
        CompressionCodecFactory.setCodecClasses(conf, (List) codecClasses);
        return conf;
    }

    private final Configuration configuration;
    private CompressionCodecFactory compressionCodecFactory;

    public DeephavenCodecFactory(List<Class<?>> codecClasses) {
        this(configurationWithCodecClasses(codecClasses));
    }

    public DeephavenCodecFactory(Configuration configuration) {
        this.configuration = configuration;
        compressionCodecFactory = new CompressionCodecFactory(configuration);
    }
    
    public Compressor getByName(String codecName) {
        return new CodecWrappingCompressor(compressionCodecFactory.getCodecByName(codecName));
    }

}
