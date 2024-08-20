//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import com.google.common.io.ByteStreams;
import io.airlift.compress.gzip.JdkGzipCodec;
import io.airlift.compress.lz4.Lz4Codec;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.zstd.ZstdCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.codec.SnappyCodec;
import org.apache.parquet.hadoop.codec.Lz4RawCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;


/**
 * Deephaven flavor of the Hadoop/Parquet CompressionCodec factory, offering support for picking codecs from
 * configuration or from the classpath (via service loaders), while still offering the ability to get a
 * CompressionCodecName enum value having loaded the codec in this way.
 */
public class DeephavenCompressorAdapterFactory {
    private static volatile DeephavenCompressorAdapterFactory INSTANCE;

    public static DeephavenCompressorAdapterFactory getInstance() {
        if (INSTANCE == null) {
            synchronized (DeephavenCompressorAdapterFactory.class) {
                if (INSTANCE == null) {
                    INSTANCE = createInstance();
                }
            }
        }
        return INSTANCE;
    }

    private static DeephavenCompressorAdapterFactory createInstance() {
        // It's important that we create an explicit hadoop configuration for these so they take precedence; they will
        // come last when added to the map, so will overwrite other codecs that match the same name / extension.
        // See org.apache.hadoop.io.compress.CompressionCodecFactory#addCodec.
        final Map<Class<? extends CompressionCodec>, CompressionCodecName> explicitConfig = Map.of(
                // Manually specify the "parquet" codec rather than the ServiceLoader-selected snappy codec,
                // which is apparently incompatible with other parquet files which use snappy. This codec
                // does use platform-specific implementations, but has native implementations for the
                // platforms we support today.
                SnappyCodec.class, CompressionCodecName.SNAPPY,

                // Use the Parquet LZ4_RAW codec, which internally uses aircompressor
                Lz4RawCodec.class, CompressionCodecName.LZ4_RAW,

                // The rest of these are aircompressor codecs that have fast / pure java implementations
                JdkGzipCodec.class, CompressionCodecName.GZIP,
                LzoCodec.class, CompressionCodecName.LZO,
                Lz4Codec.class, CompressionCodecName.LZ4,
                ZstdCodec.class, CompressionCodecName.ZSTD);
        final Configuration conf = configurationWithCodecClasses(explicitConfig.keySet());
        final CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        final Map<String, CompressionCodecName> codecToNames =
                new HashMap<>(CompressionCodecName.values().length + explicitConfig.size());
        for (final CompressionCodecName value : CompressionCodecName.values()) {
            final String name = value.getHadoopCompressionCodecClassName();
            if (name != null) {
                codecToNames.put(name, value);
            }
        }
        for (Entry<Class<? extends CompressionCodec>, CompressionCodecName> e : explicitConfig.entrySet()) {
            codecToNames.put(e.getKey().getName(), e.getValue());
        }
        return new DeephavenCompressorAdapterFactory(factory, Collections.unmodifiableMap(codecToNames));
    }

    static class CodecWrappingCompressorAdapter implements CompressorAdapter {
        private final CompressionCodec compressionCodec;
        private final CompressionCodecName compressionCodecName;

        private boolean canCreateCompressorObject;
        private volatile boolean canCreateDecompressorObject;
        private Compressor innerCompressor;

        CodecWrappingCompressorAdapter(final CompressionCodec compressionCodec,
                final CompressionCodecName compressionCodecName) {
            this.compressionCodec = Objects.requireNonNull(compressionCodec);
            this.compressionCodecName = Objects.requireNonNull(compressionCodecName);
            // We start with the assumption that we can create compressor/decompressor objects and update these flags
            // later if we fail to create them.
            canCreateCompressorObject = true;
            canCreateDecompressorObject = true;
        }

        @Override
        public OutputStream compress(final OutputStream os) throws IOException {
            if (innerCompressor == null && canCreateCompressorObject) {
                // Following will get a new compressor from the pool, or create a new one, if possible.
                innerCompressor = CodecPool.getCompressor(compressionCodec);
                canCreateCompressorObject = (innerCompressor != null);
            }
            if (!canCreateCompressorObject) {
                return compressionCodec.createOutputStream(os);
            }
            innerCompressor.reset();
            return compressionCodec.createOutputStream(os, innerCompressor);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return compressionCodecName;
        }

        @Override
        public InputStream decompress(
                final InputStream inputStream,
                final int compressedSize,
                final int uncompressedSize,
                final ResourceCache decompressorCache) throws IOException {
            final Decompressor decompressor;
            if (canCreateDecompressorObject) {
                // Currently, we only cache a single decompressor object inside the holder. If needed in the future, we
                // can cache multiple decompressor objects based on the codec name.
                final DecompressorHolder decompressorHolder = decompressorCache.get(DecompressorHolder::new);
                if (decompressorHolder.holdsDecompressor(compressionCodecName)) {
                    decompressor = decompressorHolder.getDecompressor();
                    decompressor.reset();
                } else {
                    decompressor = CodecPool.getDecompressor(compressionCodec);
                    // Not resetting decompressor here since it's either a new instance or was reset when previously
                    // returned to the pool
                    if (decompressor == null) {
                        canCreateCompressorObject = false;
                    } else {
                        decompressorHolder.setDecompressor(compressionCodecName, decompressor);
                    }
                }
            } else {
                decompressor = null;
            }
            // Note that we don't want the caller to close the decompressed stream because doing so may return the
            // decompressor to the pool.
            final InputStream limitedInputStream = ByteStreams.limit(inputStream, compressedSize);
            return new InputStreamNoClose(compressionCodec.createInputStream(limitedInputStream, decompressor));
        }

        @Override
        public void reset() {
            if (innerCompressor != null) {
                innerCompressor.reset();
            }
        }

        @Override
        public void close() {
            if (innerCompressor != null) {
                CodecPool.returnCompressor(innerCompressor);
            }
        }
    }

    private static Configuration configurationWithCodecClasses(
            final Collection<Class<? extends CompressionCodec>> codecClasses) {
        final Configuration conf = new Configuration();
        CompressionCodecFactory.setCodecClasses(conf, new ArrayList<>(codecClasses));
        return conf;
    }

    private final CompressionCodecFactory compressionCodecFactory;
    private final Map<String, CompressionCodecName> codecClassnameToCodecName;

    private DeephavenCompressorAdapterFactory(final CompressionCodecFactory compressionCodecFactory,
            final Map<String, CompressionCodecName> codecClassnameToCodecName) {
        this.compressionCodecFactory = Objects.requireNonNull(compressionCodecFactory);
        this.codecClassnameToCodecName = Objects.requireNonNull(codecClassnameToCodecName);
    }

    /**
     * Returns a compressor with the given codec name. The returned adapter can internally stateful in some cases and
     * therefore a single instance should not be re-used across files (check
     * {@link LZ4WithLZ4RawBackupCompressorAdapter} for more details).
     *
     * @param codecName the name of the codec to search for.
     * @return a compressor instance with a name matching the given codec.
     */
    public CompressorAdapter getByName(final String codecName) {
        if (codecName.equalsIgnoreCase("UNCOMPRESSED")) {
            return CompressorAdapter.PASSTHRU;
        }
        CompressionCodec codec = compressionCodecFactory.getCodecByName(codecName);
        if (codec == null) {
            if (codecName.equalsIgnoreCase("LZ4_RAW")) {
                // Hacky work-around since codec factory refers to LZ4_RAW as LZ4RAW
                codec = compressionCodecFactory.getCodecByName("LZ4RAW");
            }
            if (codec == null) {
                throw new IllegalArgumentException(
                        String.format("Failed to find CompressionCodec for codecName=%s", codecName));
            }
        }
        final CompressionCodecName ccn = codecClassnameToCodecName.get(codec.getClass().getName());
        if (ccn == null) {
            throw new IllegalArgumentException(String.format(
                    "Failed to find CompressionCodecName for codecName=%s, codec=%s, codec.getDefaultExtension()=%s",
                    codecName, codec, codec.getDefaultExtension()));
        }
        if (ccn == CompressionCodecName.LZ4) {
            return new LZ4WithLZ4RawBackupCompressorAdapter(codec, ccn);
        }
        return new CodecWrappingCompressorAdapter(codec, ccn);
    }
}
