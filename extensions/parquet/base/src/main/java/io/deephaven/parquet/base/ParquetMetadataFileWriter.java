//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

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
     * @param parquetFilePath The parquet file destination path
     * @param metadata The parquet metadata corresponding to the parquet file
     */
    void addParquetFileMetadata(String parquetFilePath, ParquetMetadata metadata);

    /**
     * Write the combined metadata files for all metadata accumulated so far and clear the list.
     *
     * @param metadataFilePath The destination path for the {@value ParquetUtils#METADATA_FILE_NAME} file
     * @param commonMetadataFilePath The destination path for the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
     */
    void writeMetadataFiles(String metadataFilePath, String commonMetadataFilePath) throws IOException;

    /**
     * Clear the list of metadata accumulated so far.
     */
    void clear();
}
