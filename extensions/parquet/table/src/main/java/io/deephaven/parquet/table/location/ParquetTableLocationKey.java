//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.locations.local.URITableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.parquet.base.ParquetFileReader;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderLoader;
import org.apache.commons.io.FilenameUtils;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;

import static io.deephaven.parquet.base.ParquetUtils.PARQUET_FILE_EXTENSION;
import static io.deephaven.base.FileUtils.convertToURI;

/**
 * {@link TableLocationKey} implementation for use with data stored in the parquet format.
 */
public class ParquetTableLocationKey extends URITableLocationKey {

    private static final String IMPLEMENTATION_NAME = ParquetTableLocationKey.class.getSimpleName();

    private ParquetFileReader fileReader;
    private ParquetMetadata metadata;
    private int[] rowGroupIndices;
    private SeekableChannelsProvider channelsProvider;

    /**
     * Construct a new ParquetTableLocationKey for the supplied {@code parquetFileUri} and {@code partitions}.
     * <p>
     * This constructor will create a new {@link SeekableChannelsProvider} for reading the file. If you have multiple
     * location keys that should share a provider, use the other constructor and set the provider manually.
     *
     * @param parquetFileUri The parquet file that backs the keyed location. Will be adjusted to an absolute path.
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param readInstructions the instructions for customizations while reading
     */
    public ParquetTableLocationKey(@NotNull final URI parquetFileUri, final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final ParquetInstructions readInstructions) {
        this(parquetFileUri, order, partitions, SeekableChannelsProviderLoader.getInstance()
                .load(parquetFileUri.getScheme(), readInstructions.getSpecialInstructions()));
    }

    /**
     * Construct a new ParquetTableLocationKey for the supplied {@code parquetFileUri} and {@code partitions}.
     *
     * @param parquetFileUri The parquet file that backs the keyed location. Will be adjusted to an absolute path.
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param readInstructions the instructions for customizations while reading, unused
     * @param channelsProvider the provider for reading the file
     * @deprecated the {@code readInstructions} parameter is unused, please
     *             use{@link #ParquetTableLocationKey(URI, int, Map, SeekableChannelsProvider)}
     */
    @Deprecated(forRemoval = true)
    public ParquetTableLocationKey(@NotNull final URI parquetFileUri, final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        this(parquetFileUri, order, partitions, channelsProvider);
    }

    /**
     * Construct a new ParquetTableLocationKey for the supplied {@code parquetFileUri} and {@code partitions}.
     *
     * @param parquetFileUri The parquet file that backs the keyed location. Will be adjusted to an absolute path.
     * @param order Explicit ordering index, taking precedence over other fields
     * @param partitions The table partitions enclosing the table location keyed by {@code this}. Note that if this
     *        parameter is {@code null}, the location will be a member of no partitions. An ordered copy of the map will
     *        be made, so the calling code is free to mutate the map after this call
     * @param channelsProvider the provider for reading the file
     */
    public ParquetTableLocationKey(
            @NotNull final URI parquetFileUri,
            final int order,
            @Nullable final Map<String, Comparable<?>> partitions,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        super(validateParquetFile(parquetFileUri), order, partitions);
        this.channelsProvider = Objects.requireNonNull(channelsProvider);
    }

    private static URI validateParquetFile(@NotNull final URI parquetFileUri) {
        if (!parquetFileUri.getRawPath().endsWith(PARQUET_FILE_EXTENSION)) {
            throw new IllegalArgumentException("Parquet file must end in " + PARQUET_FILE_EXTENSION + ", found: "
                    + parquetFileUri.getRawPath());
        }
        return parquetFileUri;
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
        return fileReader = ParquetFileReader.create(uri, channelsProvider);
    }

    /**
     * Set the {@link ParquetFileReader} that will be returned by {@link #getFileReader()}. Pass {@code null} to force
     * on-demand construction at the next invocation. Always clears cached {@link ParquetMetadata} and {@link RowGroup}
     * indices.
     *
     * @param fileReader The new {@link ParquetFileReader}
     */
    public synchronized void setFileReader(final ParquetFileReader fileReader) {
        this.fileReader = fileReader;
        this.metadata = null;
        this.rowGroupIndices = null;
    }

    /**
     * Get a previously-{@link #setMetadata(ParquetMetadata) set} or on-demand created {@link ParquetMetadata} for this
     * location key's {@code file}.
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
            throw new TableDataException("Failed to convert Parquet file metadata: " + getURI(), e);
        }
    }

    /**
     * Set the {@link ParquetMetadata} that will be returned by {@link #getMetadata()} ()}. Pass {@code null} to force
     * on-demand construction at the next invocation.
     *
     * @param metadata The new {@link ParquetMetadata}
     */
    public synchronized void setMetadata(final ParquetMetadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Get previously-{@link #setRowGroupIndices(int[]) set} or on-demand created {@link RowGroup} indices for this
     * location key's current {@link ParquetFileReader}.
     *
     * @return {@link RowGroup} indices for this location key's current {@link ParquetFileReader}.
     */
    public synchronized int[] getRowGroupIndices() {
        if (rowGroupIndices != null) {
            return rowGroupIndices;
        }
        final List<RowGroup> rowGroups = getFileReader().fileMetaData.getRow_groups();
        return rowGroupIndices = IntStream.range(0, rowGroups.size()).filter(rgi -> {
            // 1. We can safely assume there's always at least one column. Our tools will refuse to write a
            // column-less table, and other readers we've tested fail catastrophically.
            // 2. null file path means the column is local to the file the metadata was read from (which had
            // better be this file, in that case).
            // 3. We're assuming row groups are contained within a single file.
            // While it seems that row group *could* have column chunks splayed out into multiple files,
            // we're not expecting that in this code path. To support it, discovery tools should figure out
            // the row groups for a partition themselves and call setRowGroupReaders.
            final String filePath =
                    FilenameUtils.separatorsToSystem(rowGroups.get(rgi).getColumns().get(0).getFile_path());
            return filePath == null || convertToURI(filePath, false).equals(uri);
        }).toArray();
    }

    /**
     * Set the {@link RowGroup} indices that will be returned by {@link #getRowGroupIndices()}
     *
     * @param rowGroupIndices The new {@link RowGroup} indices
     */
    public synchronized void setRowGroupIndices(final int[] rowGroupIndices) {
        this.rowGroupIndices = rowGroupIndices;
    }

    @Override
    public synchronized void clear() {
        metadata = null;
        fileReader = null;
        rowGroupIndices = null;
        channelsProvider = null;
    }
}
