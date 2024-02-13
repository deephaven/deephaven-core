package io.deephaven.parquet.base;

import io.deephaven.UncheckedDeephavenException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.hadoop.ParquetFileWriter.writeMetadataFile;

/**
 * Uses the hadoop implementation of {@link org.apache.parquet.hadoop.ParquetFileWriter} to generate metadata files for
 * provided Parquet files.
 */
public final class MetadataFileWriter implements MetadataFileWriterBase {
    // TODO Please suggest a better name for this class and the interface

    private final String metadataRootDir;
    private final List<Footer> footers;

    public MetadataFileWriter(final String metadataRootDir, final File[] destinations) {
        // TODO Discuss with Ryan about this check
        // This check exists inside the hadoop code but is done at the time of writing the metadata files
        // I added this here so that we can do this check upfront to catch errors early.
        for (final File destination : destinations) {
            if (!destination.getAbsolutePath().startsWith(metadataRootDir)) {
                throw new UncheckedDeephavenException("All destinations must be contained in the provided metadata root"
                        + " directory, provided destination " + destination.getAbsolutePath() + " is not in " +
                        metadataRootDir);
            }
        }
        this.metadataRootDir = metadataRootDir;
        this.footers = new ArrayList<>(destinations.length);
    }

    /**
     * Added parquet metadata for provided parquet file.
     *
     * @param parquetFile The parquet file destination path
     * @param parquetMetadata The parquet metadata
     */
    public void addFooter(final File parquetFile, final ParquetMetadata parquetMetadata) {
        footers.add(new Footer(new Path(parquetFile.getAbsolutePath()), parquetMetadata));
    }

    public void writeMetadataFiles() {
        final Configuration conf = new Configuration();
        conf.setBoolean("fs.file.impl.disable.cache", true);

        // TODO Discuss with Ryan about this setting
        // Not setting it requires adding dependency on hadoop-auth and commons-configuration2
        // Setting it means we will not cache the filesystem instance which leads to a small performance hit but that's
        // okay since this is only used when writing the metadata files
        // But this leads to additional warnings like
        // Jan 31, 2024 6:31:23 PM org.apache.hadoop.fs.FileSystem loadFileSystems
        // WARNING: Cannot load filesystem: java.util.ServiceConfigurationError: org.apache.hadoop.fs.FileSystem:
        // Provider org.apache.hadoop.fs.viewfs.ViewFileSystem could not be instantiated
        // Jan 31, 2024 6:31:23 PM org.apache.hadoop.fs.FileSystem loadFileSystems
        // WARNING: java.lang.NoClassDefFoundError: org/apache/commons/configuration2/Configuration
        // Jan 31, 2024 6:31:23 PM org.apache.hadoop.fs.FileSystem loadFileSystems
        // WARNING: java.lang.ClassNotFoundException: org.apache.commons.configuration2.Configuration
        //
        // Other option is to move the metadata file writing code outside of parquet-hadoop and then remove the code
        // for this caching and filesystem stuff. Note that the parquet-hadoop method is deprecated, so we don't expect
        // the code to
        // change in the future.

        try {
            writeMetadataFile(conf, new Path(metadataRootDir), footers);
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Failed to write metadata files at " + metadataRootDir, e);
        }
        footers.clear();
    }
}
