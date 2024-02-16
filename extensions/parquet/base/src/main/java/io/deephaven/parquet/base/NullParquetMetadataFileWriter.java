package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;

/**
 * A no-op implementation of MetadataFileWriterBase when we don't want to write metadata files for Parquet files.
 */
public final class NullParquetMetadataFileWriter implements ParquetMetadataFileWriter {

    public static final NullParquetMetadataFileWriter INSTANCE = new NullParquetMetadataFileWriter();

    private NullParquetMetadataFileWriter() {}

    @Override
    public void addParquetFileMetadata(final File parquetFile, final ParquetMetadata metadata) {}

    @Override
    public void writeMetadataFiles(final File metadataFile, final File commonMetadataFile) {}
}

