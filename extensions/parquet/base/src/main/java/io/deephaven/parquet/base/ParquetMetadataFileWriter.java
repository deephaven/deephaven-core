package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;

/**
 * Used to write {@value ParquetUtils#METADATA_FILE_NAME} and {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files for
 * Parquet.
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
     * @param metadataFile The destination for the {@value ParquetUtils#METADATA_FILE_NAME} file
     * @param commonMetadataFile The destination for the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
     */
    void writeMetadataFiles(File metadataFile, File commonMetadataFile) throws IOException;

    /**
     * Clear the list of metadata accumulated so far.
     */
    void clear();
}
