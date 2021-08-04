package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.local.FileTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * {@link TableLocationKey} implementation for use with data stored in the parquet format.
 */
public final class ParquetTableLocationKey extends FileTableLocationKey {

    private static final String IMPLEMENTATION_NAME = ParquetTableLocationKey.class.getSimpleName();

    private ParquetFileReader fileReader;
    private ParquetMetadata metadata;
    private int[] rowGroupOrdinals;

    /**
     * Construct a new ParquetTableLocationKey for the supplied {@code file} and {@code partitions}.
     *
     * @param file       The parquet file that backs the keyed location. Will be adjusted to an absolute path.
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *                   parameter is {@code null}, the location will be a member of no partitions. An ordered copy
     *                   of the map will be made, so the calling code is free to mutate the map after this call
     *                   completes, but the partition keys and values themselves <em>must</em> be effectively immutable.
     */
    public ParquetTableLocationKey(@NotNull final File file, @Nullable final Map<String, Comparable<?>> partitions) {
        super(validateParquetFile(file), partitions);
    }

    private static File validateParquetFile(@NotNull final File file) {
        if (!file.getName().endsWith(ParquetTableWriter.PARQUET_FILE_EXTENSION)) {
            throw new IllegalArgumentException("Parquet file must end in " + ParquetTableWriter.PARQUET_FILE_EXTENSION);
        }
        return file;
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }


    /**
     * Get a previously-{@link #setFileReader(ParquetFileReader) set} or on-demand created {@link ParquetFileReader} for
     * this location key's {@code file}.
     *
     * @return A {@link ParquetFileReader} for this location key's {@code file}.
     */
    public synchronized ParquetFileReader getFileReader() {
        if (fileReader != null) {
            return fileReader;
        }
        return fileReader = ParquetTools.getParquetFileReader(file);
    }

    /**
     * Set the {@link ParquetFileReader} that will be returned by {@link #getFileReader()}.
     * Pass {@code null} to force on-demand construction at the next invocation.
     * Always clears cached {@link ParquetMetadata} and {@link RowGroup} ordinals.
     *
     * @param fileReader The new {@link ParquetFileReader}
     */
    public synchronized void setFileReader(final ParquetFileReader fileReader) {
        this.fileReader = fileReader;
        this.metadata = null;
        this.rowGroupOrdinals = null;
    }

    /**
     * Get a previously-{@link #setMetadata(ParquetMetadata) set} or on-demand created {@link ParquetMetadata} for
     * this location key's {@code file}.
     *
     * @return A {@link ParquetMetadata} for this location key's {@code file}.
     */
    public synchronized ParquetMetadata getMetadata() {
        if (metadata != null) {
            return metadata;
        }
        try {
            return metadata = new ParquetMetadataConverter().fromParquetMetadata(getFileReader().fileMetaData);
        } catch (IOException e) {
            throw new TableDataException("Failed to convert Parquet file metadata: " + getFile(), e);
        }
    }

    /**
     * Set the {@link ParquetMetadata} that will be returned by {@link #getMetadata()} ()}.
     * Pass {@code null} to force on-demand construction at the next invocation.
     *
     * @param metadata The new {@link ParquetMetadata}
     */
    public synchronized void setMetadata(final ParquetMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Get previously-{@link #setRowGroupOrdinals(int[]) set} or on-demand created
     * {@link RowGroup} ordinals for this location key's current {@link ParquetFileReader}.
     *
     * @return {@link RowGroup} ordinals for this location key's current {@link ParquetFileReader}.
     */
    public synchronized int[] getRowGroupOrdinals() {
        if (rowGroupOrdinals != null) {
            return rowGroupOrdinals;
        }
        final ParquetFileReader fileReader = getFileReader();
        final FileMetaData fileMetadata = fileReader.fileMetaData;
        return rowGroupOrdinals = fileMetadata.getRow_groups()
                .stream()
                .filter(rg -> {
                    // 1. We can safely assume there's always at least one column. Our tools will refuse to write a
                    //    column-less table, and other readers we've tested fail catastrophically.
                    // 2. null file path means the column is local to the file the metadata was read from (which had
                    //    better be this file, in that case).
                    // 3. We're assuming row groups are contained within a single file.
                    //    While it seems that row group *could* have column chunks splayed out into multiple files,
                    //    we're not expecting that in this code path. To support it, discovery tools should figure out
                    //    the row groups for a partition themselves and call setRowGroupReaders.
                    final String filePath = rg.getColumns().get(0).getFile_path();
                    return filePath == null || new File(filePath).getAbsoluteFile().equals(file);
                })
                .mapToInt(RowGroup::getOrdinal)
                .toArray();
    }

    /**
     * Set the {@link RowGroup} ordinals that will be returned by {@link #getRowGroupOrdinals()}
     *
     * @param rowGroupOrdinals The new {@link RowGroup} ordinals
     */
    public synchronized void setRowGroupOrdinals(final int[] rowGroupOrdinals) {
        this.rowGroupOrdinals = rowGroupOrdinals;
    }
}
