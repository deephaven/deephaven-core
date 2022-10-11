/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.compress;

import com.google.common.io.ByteStreams;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.*;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Deephaven flavor of the Hadoop/Parquet CompressionCodec factory, offering support for picking codecs from
 * configuration or from the classpath (via service loaders), while still offering the ability to get a
 * CompressionCodecName enum value having loaded the codec in this way.
 */
public class DeephavenCompressorAdapterFactory {

    // Default codecs to list in the configuration rather than rely on the classloader
    private static final Set<String> DEFAULT_CODECS = Set.of(
            // Manually specify the "parquet" codec rather than the ServiceLoader-selected snappy codec, which is
            // apparently incompatible with other parquet files which use snappy. This codec does use platform-specific
            // implementations, but has native implementations for the platforms we support today.
            "org.apache.parquet.hadoop.codec.SnappyCodec");
    private static final List<Class<?>> CODECS = io.deephaven.configuration.Configuration.getInstance()
            .getStringSetFromPropertyWithDefault("DeephavenCodecFactory.codecs", DEFAULT_CODECS).stream()
            .map((String className) -> {
                try {
                    return Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw new IllegalStateException("Can't find codec with name " + className);
                }
            }).collect(Collectors.toList());

    private static volatile DeephavenCompressorAdapterFactory INSTANCE;

    public static synchronized void setInstance(DeephavenCompressorAdapterFactory factory) {
        if (INSTANCE != null) {
            throw new IllegalStateException("Can't assign an instance when one is already set");
        }
        INSTANCE = factory;
    }

    public static DeephavenCompressorAdapterFactory getInstance() {
        if (INSTANCE == null) {
            synchronized (DeephavenCompressorAdapterFactory.class) {
                if (INSTANCE == null) {
                    INSTANCE = new DeephavenCompressorAdapterFactory(CODECS);
                }
            }
        }
        return INSTANCE;
    }

    public static class CodecWrappingCompressorAdapter implements CompressorAdapter {
        private final CompressionCodec compressionCodec;

        private boolean innerCompressorPooled;
        private Compressor innerCompressor;

        private CodecWrappingCompressorAdapter(CompressionCodec compressionCodec) {
            this.compressionCodec = compressionCodec;
        }

        @Override
        public OutputStream compress(OutputStream os) throws IOException {
            if (innerCompressor == null) {
                innerCompressor = CodecPool.getCompressor(compressionCodec);
                innerCompressorPooled = innerCompressor != null;
                if (!innerCompressorPooled) {
                    // Some compressors are allowed to declare they cannot be pooled. If we fail to get one
                    // then fall back on just creating a new one to hang on to.
                    innerCompressor = compressionCodec.createCompressor();
                }

                if (innerCompressor == null) {
                    return compressionCodec.createOutputStream(os);
                }

                innerCompressor.reset();
            }
            return compressionCodec.createOutputStream(os, innerCompressor);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return Stream.of(CompressionCodecName.values())
                    .filter(codec -> compressionCodec.getDefaultExtension().equals(codec.getExtension()))
                    .findAny()
                    .get();
        }

        @Override
        public BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize)
                throws IOException {
            final Decompressor decompressor = CodecPool.getDecompressor(compressionCodec);
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
                InputStream buffered = ByteStreams.limit(IOUtils.buffer(inputStream), compressedSize);
                CompressionInputStream decompressed = compressionCodec.createInputStream(buffered, decompressor);
                return BytesInput.copy(BytesInput.from(decompressed, uncompressedSize));
            } finally {
                // Always return it, the pool will decide if it should be reused or not.
                // CodecFactory has no logic around only returning after successful streams,
                // and the instance appears to leak otherwise.
                CodecPool.returnDecompressor(decompressor);
            }
        }

        @Override
        public void reset() {
            if (innerCompressor != null) {
                innerCompressor.reset();
            }
        }

        @Override
        public void close() {
            if (innerCompressor != null && innerCompressorPooled) {
                CodecPool.returnCompressor(innerCompressor);
            }
        }
    }

    private static Configuration configurationWithCodecClasses(List<Class<?>> codecClasses) {
        Configuration conf = new Configuration();
        // noinspection unchecked, rawtypes
        CompressionCodecFactory.setCodecClasses(conf, (List) codecClasses);
        return conf;
    }

    private final CompressionCodecFactory compressionCodecFactory;

    public DeephavenCompressorAdapterFactory(List<Class<?>> codecClasses) {
        this(configurationWithCodecClasses(codecClasses));
    }

    public DeephavenCompressorAdapterFactory(Configuration configuration) {
        compressionCodecFactory = new CompressionCodecFactory(configuration);
    }

    /**
     * Returns a compressor with the given codec name.
     *
     * @param codecName the name of the codec to search for.
     * @return a compressor instance with a name matching the given codec.
     */
    public CompressorAdapter getByName(String codecName) {
        if (codecName.equalsIgnoreCase("UNCOMPRESSED")) {
            return CompressorAdapter.PASSTHRU;
        }

        CompressionCodec codec = compressionCodecFactory.getCodecByName(codecName);
        if (codec == null) {
            throw new IllegalArgumentException("Failed to find a compression codec with name " + codecName);
        }
        return new CodecWrappingCompressorAdapter(codec);
    }
}
