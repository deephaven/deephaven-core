//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.annotation.Nullable;

/**
 * Holder for a decompressor object, used to manage the lifecycle of the decompressor.
 */
public interface DecompressorHolder {
    /**
     * Set the codec name and the corresponding decompressor.
     */
    default void setDecompressor(CompressionCodecName codecName, Decompressor decompressor) {
        throw new UnsupportedOperationException("setDecompressor not implemented");
    }

    /**
     * @return the codec name, or null if none is set
     */
    @Nullable
    default CompressionCodecName getCodecName() {
        throw new UnsupportedOperationException("getCodecName not implemented");
    };

    /**
     * @return the decompressor, or null if none is set
     */
    @Nullable
    default Decompressor getDecompressor() {
        throw new UnsupportedOperationException("getDecompressor not implemented");
    };

}
