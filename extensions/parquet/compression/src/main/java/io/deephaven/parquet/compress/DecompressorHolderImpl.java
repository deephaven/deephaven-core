//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;

public class DecompressorHolderImpl implements DecompressorHolder, AutoCloseable {
    private CompressionCodecName codecName;
    private Decompressor decompressor;

    @Override
    public void setDecompressor(@NotNull final CompressionCodecName codecName,
            @NotNull final Decompressor decompressor) {
        if (codecName.equals(this.codecName)) {
            throw new IllegalArgumentException("Already holding a decompressor of type " + codecName);
        }
        if (this.decompressor != null) {
            CodecPool.returnDecompressor(this.decompressor);
        }
        this.codecName = codecName;
        this.decompressor = decompressor;
    }

    @Override
    @Nullable
    public CompressionCodecName getCodecName() {
        return codecName;
    }

    @Override
    @Nullable
    public Decompressor getDecompressor() {
        return decompressor;
    }

    @Override
    public void close() {
        if (decompressor != null) {
            CodecPool.returnDecompressor(decompressor);
        }
    }
}
