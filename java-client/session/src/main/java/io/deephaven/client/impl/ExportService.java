/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.util.List;

// todo: this should not be public?

/**
 * This is a low-level service for directly
 */
public interface ExportService {

    /**
     * Creates new exports according to the {@code request}.
     *
     * @param request the request
     * @return the exports
     */
    List<Export> export(ExportsRequest request);

    // todo: support interface for cancelling
    // todo: support interface for actually executing after return
}
