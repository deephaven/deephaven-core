/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.util.List;

// todo: this should not be public?

/**
 * This is a low-level service for directly interacting with
 */
interface ExportService {

    /**
     * Creates new exports according to the {@code request}.
     *
     * @param request the request
     * @return the exports
     */
    List<Export> export(ExportsRequest request);
}
