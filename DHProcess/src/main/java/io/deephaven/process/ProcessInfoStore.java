/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.process;

import java.io.IOException;

public interface ProcessInfoStore {
    void put(ProcessInfo info) throws IOException;
}
