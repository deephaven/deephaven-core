/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import java.util.List;

interface ExportServiceRequest {
    List<Export> exports();

    Runnable send();
}
