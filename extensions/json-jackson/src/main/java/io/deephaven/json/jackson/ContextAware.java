//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.chunk.WritableChunk;

import java.util.List;

interface ContextAware {
    void setContext(List<WritableChunk<?>> out);

    void clearContext();

    int numColumns();
}
