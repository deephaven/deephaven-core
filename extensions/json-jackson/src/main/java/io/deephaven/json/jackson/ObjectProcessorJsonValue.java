//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

abstract class ObjectProcessorJsonValue<X> implements ObjectProcessor<X> {

    protected abstract JsonParser createParser(X in) throws IOException;

    private final ValueProcessor processor;

    ObjectProcessorJsonValue(ValueProcessor processor) {
        this.processor = Objects.requireNonNull(processor);
    }

    @Override
    public final int outputSize() {
        return processor.numColumns();
    }

    @Override
    public final List<Type<?>> outputTypes() {
        return processor.columnTypes().collect(Collectors.toList());
    }

    @Override
    public final void processAll(ObjectChunk<? extends X, ?> in, List<WritableChunk<?>> out) {
        processor.setContext(out);
        try {
            processAllImpl(in);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            processor.clearContext();
        }
    }

    void processAllImpl(ObjectChunk<? extends X, ?> in) throws IOException {
        for (int i = 0; i < in.size(); ++i) {
            try (final JsonParser parser = createParser(in.get(i))) {
                ValueProcessor.processFullJson(parser, processor);
            }
        }
    }
}
