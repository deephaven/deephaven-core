/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.io.Closeable;
import java.util.List;

interface ExportServiceRequest extends Closeable {
    List<Export> exports();

    // Note: not providing cancel() or Runnable as return here; we don't really want to cancel a Batch request, we'll
    // plumb through releases per export.
    void send();

    @Override
    void close();
}
