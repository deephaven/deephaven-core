package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;

/**
 * A no-op implementation of MetadataFileWriterBase when we don't want to write metadata files for Parquet files.
 */
public final class NullMetadataFileWriter implements MetadataFileWriterBase {

    public static final NullMetadataFileWriter INSTANCE = new NullMetadataFileWriter();

    private NullMetadataFileWriter() {}

    @Override
    public void addFooter(final File parquetFile, final ParquetMetadata parquetMetadata) {}

    @Override
    public void writeMetadataFiles() {}
}

