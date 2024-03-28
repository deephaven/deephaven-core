//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.StringOptions;
import io.deephaven.json.jackson.ObjectValueProcessor.ToObject;
import io.deephaven.qst.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class StringMixin extends Mixin<StringOptions> implements ToObject<String> {

    public StringMixin(StringOptions options, JsonFactory factory) {
        super(factory, options);
    }

    @Override
    public int numColumns() {
        return 1;
    }

    @Override
    public Stream<List<String>> paths() {
        return Stream.of(List.of());
    }

    @Override
    public Stream<Type<?>> outputTypes() {
        return Stream.of(Type.stringType());
    }

    @Override
    public ValueProcessor processor(String context, List<WritableChunk<?>> out) {
        return ObjectValueProcessor.of(out.get(0).asWritableObjectChunk(), this);
    }

    @Override
    public String parseValue(JsonParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_STRING:
            case FIELD_NAME:
                return parseFromString(parser);
            case VALUE_NUMBER_INT:
                return parseFromInt(parser);
            case VALUE_NUMBER_FLOAT:
                return parseFromDecimal(parser);
            case VALUE_TRUE:
            case VALUE_FALSE:
                return parseFromBool(parser);
            case VALUE_NULL:
                return parseFromNull(parser);
        }
        throw Parsing.mismatch(parser, String.class);
    }

    @Override
    public String parseMissing(JsonParser parser) throws IOException {
        return parseFromMissing(parser);
    }

    @Override
    RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out) {
        return new RepeaterGenericImpl<>(out.get(0).asWritableObjectChunk()::add, allowMissing, allowNull, null,
                null, this, String.class, String[].class);
    }

    private String parseFromString(JsonParser parser) throws IOException {
        if (!options.allowString()) {
            throw Parsing.mismatch(parser, String.class);
        }
        return Parsing.parseStringAsString(parser);
    }

    private String parseFromInt(JsonParser parser) throws IOException {
        if (!options.allowNumberInt()) {
            throw Parsing.mismatch(parser, String.class);
        }
        return Parsing.parseIntAsString(parser);
    }

    private String parseFromDecimal(JsonParser parser) throws IOException {
        if (!options.allowDecimal()) {
            throw Parsing.mismatch(parser, String.class);
        }
        return Parsing.parseDecimalAsString(parser);
    }

    private String parseFromBool(JsonParser parser) throws IOException {
        if (!options.allowBool()) {
            throw Parsing.mismatch(parser, String.class);
        }
        return Parsing.parseBoolAsString(parser);
    }

    private String parseFromNull(JsonParser parser) throws IOException {
        if (!options.allowNull()) {
            throw Parsing.mismatch(parser, String.class);
        }
        return options.onNull().orElse(null);
    }

    private String parseFromMissing(JsonParser parser) throws IOException {
        if (!options.allowMissing()) {
            throw Parsing.mismatchMissing(parser, String.class);
        }
        return options.onMissing().orElse(null);
    }
}
