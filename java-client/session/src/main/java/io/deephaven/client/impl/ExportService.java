/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

/**
 * This is a low-level internal service for interacting with batch requests.
 */
interface ExportService {

    ExportServiceRequest exportRequest(ExportsRequest request);
}
