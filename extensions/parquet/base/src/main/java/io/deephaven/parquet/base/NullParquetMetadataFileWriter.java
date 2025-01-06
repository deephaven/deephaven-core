//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.CompletableOutputStream;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.net.URI;

/**
 * A no-op implementation of MetadataFileWriterBase when we don't want to write metadata files for Parquet files.
 */
public enum NullParquetMetadataFileWriter implements ParquetMetadataFileWriter {

    INSTANCE;

    @Override
    public void addParquetFileMetadata(final URI parquetFileURI, final ParquetMetadata metadata) {}

    @Override
    public void writeMetadataFiles(
            final CompletableOutputStream metadataOutputStream,
            final CompletableOutputStream commonMetadataOutputStream) {}
}
