/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

public interface HasExportId extends HasTicketId, HasPathId {

    /**
     * Get the export ID.
     *
     * @return the export ID
     */
    ExportId exportId();
}
