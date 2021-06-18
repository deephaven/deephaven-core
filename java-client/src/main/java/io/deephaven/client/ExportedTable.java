package io.deephaven.client;

import io.deephaven.qst.table.Table;
import java.io.Closeable;

/**
 * An exported table represents a server side table that is referencable.
 *
 * <p>
 * The client must maintain strong ownership of references and close it when done.
 */
public interface ExportedTable extends Closeable {

    /**
     * The table.
     *
     * @return the table
     */
    Table table();

    /**
     * Release the export.
     *
     * <p>
     * Note: this will only trigger a server-side release when all client references have been
     * released.
     */
    void release();

    /**
     * Close the export, equivalent to {@link #release()}.
     */
    @Override
    default void close() {
        release();
    }

    /**
     * Creates a new export.
     *
     * @return the new reference
     */
    ExportedTable newRef();
}
