package io.deephaven.extensions.barrage.util;

import io.deephaven.extensions.barrage.ColumnConversionMode;

public interface StreamReaderOptions {
    boolean useDeephavenNulls();

    ColumnConversionMode columnConversionMode();
}
