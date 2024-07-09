//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;
import java.util.stream.Stream;

interface ContextAware {
    void setContext(List<WritableChunk<?>> out);

    void clearContext();

    int numColumns();

    Stream<Type<?>> columnTypes();
}
