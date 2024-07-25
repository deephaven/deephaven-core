//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.net.URI;

/**
 * Used to write {@value ParquetUtils#METADATA_FILE_NAME} and {@value ParquetUtils#COMMON_METADATA_FILE_NAME} files for
 * Parquet.
 */
public interface ParquetMetadataFileWriter {

    /**
     * Add the parquet metadata for the provided parquet file to the list of metadata to be written to combined metadata
     * files.
     *
     * @param parquetFileURI The parquet file destination URI
     * @param metadata The parquet metadata corresponding to the parquet file
     */
    void addParquetFileMetadata(URI parquetFileURI, ParquetMetadata metadata);

    /**
     * Write the combined metadata files for all metadata accumulated so far and clear the list.
     *
     * @param metadataFileURI The destination URI for the {@value ParquetUtils#METADATA_FILE_NAME} file
     * @param commonMetadataFileURI The destination URI for the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
     */
    void writeMetadataFiles(URI metadataFileURI, URI commonMetadataFileURI) throws IOException;

    /**
     * Clear the list of metadata accumulated so far.
     */
    void clear();
}
