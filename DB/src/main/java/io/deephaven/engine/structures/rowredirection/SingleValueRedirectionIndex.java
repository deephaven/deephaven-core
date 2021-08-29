package io.deephaven.engine.structures.rowredirection;

public interface SingleValueRedirectionIndex extends RedirectionIndex {
    void setValue(long value);

    long getValue();
}
