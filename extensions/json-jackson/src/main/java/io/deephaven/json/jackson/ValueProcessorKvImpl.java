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

final class ValueProcessorKvImpl implements ValueProcessor {

    public static void processKeyValues2(
            JsonParser parser,
            RepeaterProcessor keyProcessor,
            RepeaterProcessor valueProcessor,
            Runnable processElementCallback) throws IOException {
        Parsing.assertCurrentToken(parser, JsonToken.START_OBJECT);
        final Context keyContext = keyProcessor.context();
        final Context valueContext = valueProcessor.context();
        keyContext.init(parser);
        valueContext.init(parser);
        parser.nextToken();
        int ix;
        for (ix = 0; !parser.hasToken(JsonToken.END_OBJECT); ++ix) {
            Parsing.assertCurrentToken(parser, JsonToken.FIELD_NAME);
            keyContext.processElement(parser, ix);
            parser.nextToken();
            valueContext.processElement(parser, ix);
            parser.nextToken();
            if (processElementCallback != null) {
                processElementCallback.run();
            }
        }
        keyContext.done(parser, ix);
        valueContext.done(parser, ix);
    }

    private final RepeaterProcessor keyProcessor;
    private final RepeaterProcessor valueProcessor;

    public ValueProcessorKvImpl(RepeaterProcessor keyProcessor, RepeaterProcessor valueProcessor) {
        this.keyProcessor = Objects.requireNonNull(keyProcessor);
        this.valueProcessor = Objects.requireNonNull(valueProcessor);
    }

    @Override
    public void setContext(List<WritableChunk<?>> out) {
        final int keySize = keyProcessor.numColumns();
        keyProcessor.setContext(out.subList(0, keySize));
        valueProcessor.setContext(out.subList(keySize, keySize + valueProcessor.numColumns()));
    }

    @Override
    public void clearContext() {
        keyProcessor.clearContext();
        valueProcessor.clearContext();
    }

    @Override
    public int numColumns() {
        return keyProcessor.numColumns() + valueProcessor.numColumns();
    }

    @Override
    public void processCurrentValue(JsonParser parser) throws IOException {
        if (parser.hasToken(JsonToken.VALUE_NULL)) {
            keyProcessor.processNullRepeater(parser);
            valueProcessor.processNullRepeater(parser);
            return;
        }
        processKeyValues2(parser, keyProcessor, valueProcessor, null);
    }

    @Override
    public void processMissing(JsonParser parser) throws IOException {
        keyProcessor.processMissingRepeater(parser);
        valueProcessor.processMissingRepeater(parser);
    }
}
