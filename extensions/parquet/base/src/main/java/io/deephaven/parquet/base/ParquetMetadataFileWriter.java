//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.CompletableOutputStream;
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
     * Write the combined metadata to the provided streams and clear the metadata accumulated so far. The output streams
     * should be marked as {@link CompletableOutputStream#done()} after writing is finished.
     *
     * @param metadataOutputStream The output stream for the {@value ParquetUtils#METADATA_FILE_NAME} file
     * @param commonMetadataOutputStream The output stream for the {@value ParquetUtils#COMMON_METADATA_FILE_NAME} file
     */
    void writeMetadataFiles(
            CompletableOutputStream metadataOutputStream,
            CompletableOutputStream commonMetadataOutputStream) throws IOException;
}
