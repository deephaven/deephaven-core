package io.deephaven.internals;

public interface JdkInternals {
    Unsafe getUnsafe();

    DirectMemoryStats getDirectMemoryStats();
}
