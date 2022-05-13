package io.deephaven.parquet.base.compress;

import org.apache.parquet.hadoop.codec.ZstandardCodec;

/**
 * Provides an alternative codec name of "ZSTD" instead of the superclass's
 * "ZSTANDARD".
 */
public class ZstdCodec extends ZstandardCodec {
//    @Override
//    public String getDefaultExtension() {
//        return ".zstd";
//    }
}
