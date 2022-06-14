/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.extensions.barrage.util;

import io.deephaven.extensions.barrage.ColumnConversionMode;

public interface StreamReaderOptions {
    boolean useDeephavenNulls();

    ColumnConversionMode columnConversionMode();

    int batchSize();
}
