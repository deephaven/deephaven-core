package io.deephaven.db.v2.locations.parquet.local;

import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.local.FileTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetTableWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.util.Map;

/**
 * {@link TableLocationKey} implementation for use with data stored in the parquet format.
 */
public final class ParquetTableLocationKey extends FileTableLocationKey {

    private static final String IMPLEMENTATION_NAME = ParquetTableLocationKey.class.getSimpleName();

    /**
     * Construct a new ParquetTableLocationKey for the supplied {@code file} and {@code partitions}.
     *
     * @param file        The parquet file that backs the keyed location. Will be adjusted to an absolute path.
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
}
