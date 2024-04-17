//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

final class ArrayValueProcessor implements ValueProcessor {

    private final RepeaterProcessor elementProcessor;

    ArrayValueProcessor(RepeaterProcessor elementProcessor) {
        this.elementProcessor = Objects.requireNonNull(elementProcessor);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        elementProcessor.setContext(out);
    }

    @Override
    public void clearContext() {
        elementProcessor.clearContext();
    }

    @Override
    public int numColumns() {
        return elementProcessor.numColumns();
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        if (parser.hasToken(JsonToken.VALUE_NULL)) {
            elementProcessor.processNullRepeater(parser);
            return;
        }
        RepeaterProcessor.processArray(parser, elementProcessor);
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        elementProcessor.processMissingRepeater(parser);
    }
}
