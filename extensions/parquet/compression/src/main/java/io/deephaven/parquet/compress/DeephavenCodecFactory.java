package io.deephaven.parquet.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
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
