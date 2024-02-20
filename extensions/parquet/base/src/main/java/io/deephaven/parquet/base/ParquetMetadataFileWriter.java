package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;

/**
 * Used to write _metadata and _common_metadata files for Parquet.
 */
public interface ParquetMetadataFileWriter {

    /**
     * Add the parquet metadata for the provided parquet file to the list of metadata to be written to combined metadata
     * files.
     *
     * @param parquetFile The parquet file destination path
     * @param metadata The parquet metadata corresponding to the parquet file
     */
    void addParquetFileMetadata(File parquetFile, ParquetMetadata metadata);

    /**
     * Write the combined metadata files for all metadata accumulated so far and clear the list.
     *
     * @param metadataFile The destination file for the _metadata file
     * @param commonMetadataFile The destination file for the _common_metadata file
     */
    void writeMetadataFiles(File metadataFile, File commonMetadataFile) throws IOException;

    /**
     * Clear the list of metadata accumulated so far.
     */
    void clear();
}
