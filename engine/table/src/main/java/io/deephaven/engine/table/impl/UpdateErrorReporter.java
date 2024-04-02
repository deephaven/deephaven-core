//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import java.io.IOException;

public interface UpdateErrorReporter {
    void reportUpdateError(Throwable t) throws IOException;
}
