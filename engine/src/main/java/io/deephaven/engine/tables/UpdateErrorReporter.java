/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.tables;

import java.io.IOException;

public interface UpdateErrorReporter {
    void reportUpdateError(Throwable t) throws IOException;
}
