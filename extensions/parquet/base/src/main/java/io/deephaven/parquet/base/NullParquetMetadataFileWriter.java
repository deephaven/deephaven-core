//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

/**
 * A no-op implementation of MetadataFileWriterBase when we don't want to write metadata files for Parquet files.
 */
public enum NullParquetMetadataFileWriter implements ParquetMetadataFileWriter {

    INSTANCE;

    @Override
    public void addParquetFileMetadata(final String parquetFilePath, final ParquetMetadata metadata) {}

    @Override
    public void writeMetadataFiles(final String metadataFilePath, final String commonMetadataFilePath) {}

    @Override
    public void clear() {}
}
