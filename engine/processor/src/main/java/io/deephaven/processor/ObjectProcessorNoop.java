/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.List;

final class ObjectProcessorNoop<T> implements ObjectProcessor<T> {
    private final List<Type<?>> outputTypes;
    private final boolean fillWithNullValue;

    ObjectProcessorNoop(List<Type<?>> outputTypes, boolean fillWithNullValue) {
        this.outputTypes = List.copyOf(outputTypes);
        this.fillWithNullValue = fillWithNullValue;
    }

    @Override
    public List<Type<?>> outputTypes() {
        return outputTypes;
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        if (fillWithNullValue) {
            for (WritableChunk<?> chunk : out) {
                chunk.fillWithNullValue(chunk.size(), in.size());
            }
        }
        for (WritableChunk<?> chunk : out) {
            chunk.setSize(chunk.size() + in.size());
        }
    }
}
