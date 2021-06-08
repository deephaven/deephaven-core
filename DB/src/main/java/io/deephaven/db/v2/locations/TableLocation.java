/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.log.LogOutput;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

/**
 *<br> =====================================================================================================================
 *<br>
 *<br> Building block for Deephaven file-based tables, with helper methods for discovering locations and their sizes.
 *<br>
 *<br> ================================================= INTERFACE =========================================================
 *<br>
 *<br> A location specifies the column and size data location for a splayed table.  The location may be either:
 *<br> (1) the sole location of a stand-alone splayed table, or
 *<br> (2) a single location of a nested partitioned table, in which case it belongs to an internal partition and a column
 *         partition.
 *<br>
 *<br> ================================================== LAYOUTS ==========================================================
 *<br>
 *<br> There are exactly two layouts in use for file-based tables:
 *<br>
 *<br> ---------------------------------------------------------------------------------------------------------------------
 *<br> Splayed: (stand-alone)
 *<br> ---------------------------------------------------------------------------------------------------------------------
 *<br> Such tables have exactly one location.
 *<br>
 *<br> The layout looks like:
 *<br> L0  ROOT (e.g. /db/&lt;namespace type&gt;/&lt;namespace name&gt;/Tables)
 *<br> L1  &lt;table name&gt;
 *<br> L2  COLUMN FILES [&lt;column name&gt;.[dat, ovr, bytes, sym, sym.bytes]], SIZE FILE [table.size]
 *<br>
 *<br> ---------------------------------------------------------------------------------------------------------------------
 *<br> Nested Partitioned:
 *<br> ---------------------------------------------------------------------------------------------------------------------
 *<br> Such tables are horizontally partitioned at two levels:
 *<br>  - The first ("internal") partitioning divides data into manageable fragments or distinct streams.
 *<br>  - The second ("column") partitioning specifies a String column (named by the table's definition, by convention
 *        nearly-always "Date").
 *<br> The locations in use by such a table are identical to those used by a splayed table, except that they are contained
 *     within a column partition, which is in turn contained within an internal partition.
 *<br>
 *<br> The layout looks like:
 *<br>  L0  ROOT (e.g. /db/&lt;namespace type&gt;/&lt;namespace name&gt;/Partitions)
 *<br>  L1  "Internal" Partitions (e.g. "0", "1", ...)
 *<br>  L2  "Column" Partitions (e.g. "2011-10-13", "2011-10-14", ...)
 *<br>  L3  &lt;table name&gt;
 *<br>  L4  COLUMN FILES [&lt;column name&gt;.[dat, ovr, bytes, sym, sym.bytes]], METADATA FILE [table.size]
 *<br>
 *<br> ================================================== COMMENTS =========================================================
 *<br>
 *<br> Future work may allow more fields from TableLocationKey or TableLocationState to be accessed as columns.
 *<br>
 *<br> =====================================================================================================================
 */
public interface TableLocation<CLT extends ColumnLocation> extends TableLocationKey, TableLocationState {

    /**
     * Listener interface for anything that wants to know about changes to a location.
     */
    interface Listener extends BasicTableDataListener {

        /**
         * Notify the listener that the table location has been updated.  This may be called "spuriously," i.e. in cases
         * where there has been no substantive update since the last handleUpdate() invocation.  Implementations should
         * use appropriate measures to avoid reacting to spurious updates.
         */
        void handleUpdate();
    }

    /**
     * @return A TableKey instance for the enclosing table
     */
    @NotNull TableKey getTableKey();

    /**
     * Enumeration of possible table location formats.
     */
    enum Format {
        /**
         * The Apache Parquet columnar format.
         */
        PARQUET
    }

    /**
     * Get the format that was used to persist this table location.
     *
     * @return The format for this table location
     */
    @NotNull Format getFormat();

    /**
     * Does this location support subscriptions? That is, can this location ever have ticking data?
     * @return True if this location supports subscriptions
     */
    boolean supportsSubscriptions();

    /**
     * <p>Subscribe to pushed location updates. Subscribing more than once with the same listener without an
     * intervening unsubscribe is an error, and may result in undefined behavior.
     * <p>This is a possibly asynchronous operation - listener will receive 1 or more handleUpdate callbacks,
     * followed by 0 or 1 handleException callbacks during invocation and continuing after completion, on a thread
     * determined by the implementation.  Don't hold a lock that prevents notification delivery while subscribing!
     * <p>This method only guarantees eventually consistent state.  To force a state update, use refresh() after
     * subscription completes.
     *
     * @param listener A listener
     */
    void subscribe(@NotNull Listener listener);

    /**
     * Unsubscribe from pushed location updates.
     *
     * @param listener The listener to forget about
     */
    void unsubscribe(@NotNull Listener listener);

    /**
     * Initialize or refresh state information.
     */
    void refresh();

    /**
     * @param name The column name
     * @return The ColumnLocation for the defined column under this table location
     */
    @NotNull CLT getColumnLocation(@NotNull CharSequence name);

    //------------------------------------------------------------------------------------------------------------------
    // LogOutputAppendable implementation / toString() override helper
    //------------------------------------------------------------------------------------------------------------------

    @Override
    @FinalDefault
    default LogOutput append(@NotNull final LogOutput logOutput) {
        return logOutput.append(getTableKey())
                .append(':').append(getImplementationName())
                .append('[').append((getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition()))
                .append('/').append(getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                .append(']');
    }

    @Override
    @FinalDefault
    default String toStringHelper() {
        return getTableKey().toString()
                + ':' + getImplementationName()
                + '[' + (getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition())
                + '/' + (getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                + ']';
    }

    /**
     * Format the table key without implementation specific bits.
     * @return a formatted string
     */
    @FinalDefault
    default String toGenericString() {
        return "TableLocation"
                + '[' + getTableKey().getNamespace()
                + '/' + getTableKey().getTableName()
                + '/' + getTableKey().getTableType().getDescriptor()
                + "]:[" + (getInternalPartition() == NULL_PARTITION ? "" : getInternalPartition())
                + '/' + (getColumnPartition() == NULL_PARTITION ? "" : getColumnPartition())
                + ']';
    }

    /**
     * Optional toString path with more implementation detail.
     * @return detailed conversion to string
     */
    @FinalDefault
    default String toStringDetailed() {
        return toStringHelper();
    }
}
