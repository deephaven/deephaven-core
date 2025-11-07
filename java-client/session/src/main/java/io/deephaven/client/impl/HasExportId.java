//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

public interface HasExportId extends HasTypedTicket, HasPathId {

    /**
     * Get the export ID.
     *
     * @return the export ID
     */
    ExportId exportId();
}
