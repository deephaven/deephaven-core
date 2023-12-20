/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.qst.type.Type;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

final class ObjectProcessorCombined<T> implements ObjectProcessor<T> {
    private final List<ObjectProcessor<? super T>> processors;

    ObjectProcessorCombined(List<ObjectProcessor<? super T>> processors) {
        this.processors = List.copyOf(processors);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return processors.stream()
                .map(ObjectProcessor::outputTypes)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        int outIx = 0;
        for (ObjectProcessor<? super T> processor : processors) {
            final int toIx = outIx + processor.outputTypes().size();
            processor.processAll(in, out.subList(outIx, toIx));
            outIx = toIx;
        }
    }
}
