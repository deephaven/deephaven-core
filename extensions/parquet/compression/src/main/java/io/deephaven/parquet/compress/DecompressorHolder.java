//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import io.deephaven.util.SafeCloseable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

public class DecompressorHolder implements SafeCloseable {
    private CompressionCodecName codecName;
    private Decompressor decompressor;

    DecompressorHolder() {}

    /**
     * Set the codec name and the corresponding decompressor.
     */
    void setDecompressor(final CompressionCodecName codecName, final Decompressor decompressor) {
        if (decompressor == null || codecName == null) {
            throw new IllegalArgumentException("Setting a null decompressor is not allowed.");
        }
        if (codecName.equals(this.codecName)) {
            throw new IllegalArgumentException("Already holding a decompressor of type " + codecName);
        }
        if (this.decompressor != null) {
            CodecPool.returnDecompressor(this.decompressor);
        }
        this.codecName = codecName;
        this.decompressor = decompressor;
    }

    /**
     * Check if the holder holds a decompressor for the given codec name.
     */
    boolean holdsDecompressor(@NotNull final CompressionCodecName codecName) {
        return decompressor != null && codecName.equals(this.codecName);
    }

    /**
     * @return the decompressor, or null if none is set
     */
    @Nullable
    Decompressor getDecompressor() {
        return decompressor;
    }

    @Override
    public void close() {
        if (decompressor != null) {
            // No need to reset the decompressor before returning it to the pool; CodecPool.returnDecompressor resets it
            // internally.
            CodecPool.returnDecompressor(decompressor);
            codecName = null;
            decompressor = null;
        }
    }
}
