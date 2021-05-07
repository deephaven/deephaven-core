package io.deephaven.process;

import java.io.IOException;

public interface ProcessInfoStore {
    void put(ProcessInfo info) throws IOException;
}
