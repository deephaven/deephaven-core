/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.compress.codec;

/**
 * Provides an alternative file extension of ".lzo", while the subclass offers ".lzo_deflate". This is necessary to
 * fully replace functionality of the non-ServiceLoader compression codec factory.
 */
public class LzoCodec extends io.airlift.compress.lzo.LzoCodec {
    @Override
    public String getDefaultExtension() {
        return ".lzo";
    }
}
