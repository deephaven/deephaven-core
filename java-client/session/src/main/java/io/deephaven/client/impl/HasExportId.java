package io.deephaven.client.impl;

public interface HasExportId extends HasTicketId, HasPathId {

    /**
     * Get the export ID.
     *
     * @return the export ID
     */
    ExportId exportId();
}
