//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.jackson.RepeaterProcessor.Context;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

final class ValueProcessorArrayImpl implements ValueProcessor {

    static void processArray2(
            JsonParser parser,
            RepeaterProcessor elementProcessor,
            Runnable processElementCallback) throws IOException {
        Parsing.assertCurrentToken(parser, JsonToken.START_ARRAY);
        final Context context = elementProcessor.context();
        context.init(parser);
        parser.nextToken();
        int ix;
        for (ix = 0; !parser.hasToken(JsonToken.END_ARRAY); ++ix) {
            context.processElement(parser, ix);
            parser.nextToken();
            if (processElementCallback != null) {
                processElementCallback.run();
            }
        }
        context.done(parser, ix);
    }

    private final RepeaterProcessor elementProcessor;

    ValueProcessorArrayImpl(RepeaterProcessor elementProcessor) {
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
        processArray2(parser, elementProcessor, null);
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        elementProcessor.processMissingRepeater(parser);
    }
}
