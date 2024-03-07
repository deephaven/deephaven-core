package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;

/**
 * A no-op implementation of MetadataFileWriterBase when we don't want to write metadata files for Parquet files.
 */
public enum NullParquetMetadataFileWriter implements ParquetMetadataFileWriter {

    INSTANCE;

    @Override
    public void addParquetFileMetadata(final File parquetFile, final ParquetMetadata metadata) {}

    @Override
    public void writeMetadataFiles(final File metadataFile, final File commonMetadataFile) {}

    @Override
    public void clear() {}
}
