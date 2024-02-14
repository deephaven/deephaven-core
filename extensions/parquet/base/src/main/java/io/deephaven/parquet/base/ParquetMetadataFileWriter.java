package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;

/**
 * Used to write _metadata and _common_metadata files for Parquet.
 */
public interface ParquetMetadataFileWriter {

    /**
     * Add the parquet metadata for the provided parquet file to the list of metadata to be written to combined metadata
     * files.
     *
     * @param parquetFile The parquet file destination path
     * @param parquetMetadata The parquet metadata corresponding to the parquet file
     */
    void addFooter(File parquetFile, ParquetMetadata parquetMetadata);

    /**
     * Write the combined metadata files for all metadata accumulated so far and clear the list.
     */
    void writeMetadataFiles();
}
