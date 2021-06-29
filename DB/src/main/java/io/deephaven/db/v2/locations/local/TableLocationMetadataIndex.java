package io.deephaven.db.v2.locations.local;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.locations.TableLocationState;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Table location metadata index support, in order to avoid depending on filesystem metadata caching for performance
 * where possible.
 */
public class TableLocationMetadataIndex {

    private static final int MAGIC_NUMBER = 0xDB2BB2DB;

    private static final int VERSION_1_INITIAL = 1;
    private static final int VERSION_2_ADDS_FORMAT = 2;

    private static final int FIRST_SUPPORTED_VERSION = VERSION_1_INITIAL;
    private static final int CURRENT_VERSION = VERSION_2_ADDS_FORMAT;

    /**
     * Immutable snapshot of a table location, providing access to all key and state fields, constructed using a
     * consistent copy of the state fields.
     */
    public static final class TableLocationSnapshot implements TableLocationKey {

        private final TableLocationLookupKey.Immutable key;
        private final TableLocation.Format format;
        private final long size;
        private final long lastModifiedTimeMillis;

        private TableLocationSnapshot(@NotNull final TableLocation tableLocation) {
            key = new TableLocationLookupKey.Immutable(tableLocation);
            format = tableLocation.getFormat();
            synchronized (tableLocation.getStateLock()) {
                size = tableLocation.getSize();
                lastModifiedTimeMillis = tableLocation.getLastModifiedTimeMillis();
            }
        }

        @VisibleForTesting
        public TableLocationSnapshot(@NotNull final String internalPartition,
                                     @NotNull final String columnPartition,
                                     @NotNull final TableLocation.Format format,
                                     final long size,
                                     final long lastModifiedTimeMillis) {
            key = new TableLocationLookupKey.Immutable(internalPartition, columnPartition);
            this.format = format;
            this.size = size;
            this.lastModifiedTimeMillis = lastModifiedTimeMillis;
        }

        @NotNull
        @Override
        public final String getInternalPartition() {
            return key.getInternalPartition();
        }

        @NotNull
        @Override
        public final String getColumnPartition() {
            return key.getColumnPartition();
        }

        /**
         * See {@link TableLocation#getFormat()}.
         *
         * @return The snapshotted format
         */
        public final TableLocation.Format getFormat() {
            return format;
        }

        /**
         * See {@link TableLocationState#getSize()}.
         *
         * @return The snapshotted size
         */
        public final long getSize() {
            return size;
        }

        /**
         * See {@link TableLocationState#getLastModifiedTimeMillis()}.
         *
         * @return The snapshotted last modified time in milliseconds
         */
        public final long getLastModifiedTimeMillis() {
            return lastModifiedTimeMillis;
        }

        private void writeTo(@NotNull final DataOutput out) throws IOException {
            out.writeUTF(getInternalPartition());
            out.writeUTF(getColumnPartition());
            out.writeLong(size);
            out.writeLong(lastModifiedTimeMillis);
        }

        private static void writeTo(@NotNull final TableLocation tableLocation, @NotNull final DataOutput out) throws IOException {
            out.writeUTF(tableLocation.getInternalPartition().toString());
            out.writeUTF(tableLocation.getColumnPartition().toString());
            out.writeUTF(tableLocation.getFormat().name());
            final long size;
            final long lastModifiedTimeMillis;
            synchronized (tableLocation.getStateLock()) {
                size = tableLocation.getSize();
                lastModifiedTimeMillis = tableLocation.getLastModifiedTimeMillis();
            }
            out.writeLong(size);
            out.writeLong(lastModifiedTimeMillis);
        }

        private static TableLocationSnapshot readFrom(@NotNull final DataInput in, final int version) throws IOException {
            final String internalPartition = in.readUTF();
            final String columnPartition = in.readUTF();
            final TableLocation.Format format = TableLocation.Format.valueOf(in.readUTF());
            final long size = in.readLong();
            final long lastModifiedTimeMillis = in.readLong();
            return new TableLocationSnapshot(internalPartition, columnPartition, format, size, lastModifiedTimeMillis);
        }
    }

    private final Map<TableLocationKey, TableLocationSnapshot> tableLocationSnapshots;

    /**
     * Construct an index by snapshotting each member of a collection of {@link TableLocation}s.
     *
     * @param tableLocations The table locations to snapshot
     */
    TableLocationMetadataIndex(@NotNull final Collection<? extends TableLocation> tableLocations) {
        this(tableLocations.stream().map(TableLocationSnapshot::new).toArray(TableLocationSnapshot[]::new));
    }

    /**
     * Internal-only constructor from an array of snapshots.
     *
     * @param tableLocationSnapshots The array of snapshots
     */
    @VisibleForTesting
    public TableLocationMetadataIndex(@NotNull final TableLocationSnapshot[] tableLocationSnapshots) {
        final KeyedObjectHashMap<TableLocationKey, TableLocationSnapshot> internalMap = new KeyedObjectHashMap<>(TableLocationKey.getKeyedObjectKey());
        Arrays.stream(tableLocationSnapshots).filter(s -> s.getSize() != TableLocationState.NULL_SIZE && s.getSize() != 0).forEach(internalMap::add);
        this.tableLocationSnapshots = Collections.unmodifiableMap(internalMap);
    }

    /**
     * Get all {@link TableLocationSnapshot}s for this index.
     *
     * @return The snapshots
     */
    Collection<TableLocationSnapshot> getTableLocationSnapshots() {
        return tableLocationSnapshots.values();
    }

    /**
     * Get the {@link TableLocationSnapshot} for the specified {@link TableLocationKey} if it exists.
     *
     * @param tableLocationKey The location key
     * @return The snapshot if it exists, else null
     */
    TableLocationSnapshot getTableLocationSnapshot(@NotNull final TableLocationKey tableLocationKey) {
        return tableLocationSnapshots.get(tableLocationKey);
    }

    final void writeTo(@NotNull final DataOutput out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(CURRENT_VERSION);
        out.writeInt(tableLocationSnapshots.size());
        for (@NotNull final TableLocationSnapshot tableLocationSnapshot : tableLocationSnapshots.values()) {
            tableLocationSnapshot.writeTo(out);
        }
    }

    static void writeTo(@NotNull final Collection<? extends TableLocation> tableLocations, @NotNull final DataOutput out) throws IOException {
        out.writeInt(MAGIC_NUMBER);
        out.writeInt(CURRENT_VERSION);
        out.writeInt(tableLocations.size());
        for (@NotNull final TableLocation tableLocation : tableLocations) {
            TableLocationSnapshot.writeTo(tableLocation, out);
        }
    }

    static TableLocationMetadataIndex readFrom(@NotNull final DataInput in) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IllegalStateException("Invalid magic number 0x" + Integer.toHexString(magicNumber).toUpperCase()
                    + ", expected 0x" + Integer.toHexString(MAGIC_NUMBER).toUpperCase());
        }
        final int version = in.readInt();
        if (version < FIRST_SUPPORTED_VERSION || version > CURRENT_VERSION) {
            throw new IllegalStateException("Invalid version " + version + ", expected in range [" + FIRST_SUPPORTED_VERSION + ", " + CURRENT_VERSION + ']');
        }
        final int snapshotCount = in.readInt();
        final TableLocationSnapshot[] snapshots = new TableLocationSnapshot[snapshotCount];
        for (int si = 0; si < snapshotCount; ++si) {
            snapshots[si] = TableLocationSnapshot.readFrom(in, version);
        }
        return new TableLocationMetadataIndex(snapshots);
    }
}
