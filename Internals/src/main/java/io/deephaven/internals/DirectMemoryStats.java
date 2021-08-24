package io.deephaven.internals;

public interface DirectMemoryStats {
    long maxDirectMemory();

    long getMemoryUsed();
}
