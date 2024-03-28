//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import io.deephaven.json.ObjectFieldOptions;
import io.deephaven.json.ObjectFieldOptions.RepeatedBehavior;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.jackson.PathToSingleValue.ArrayIndex;
import io.deephaven.json.jackson.PathToSingleValue.ObjectField;
import io.deephaven.json.jackson.PathToSingleValue.Path;
import io.deephaven.json.jackson.ValueProcessor.FieldProcess;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

final class NavContext {

    interface JsonProcess {

        void process(JsonParser parser) throws IOException;
    }

    private static class State implements FieldProcess {
        private final ObjectOptions options;
        private final ObjectFieldOptions field;
        private final JsonProcess fieldProcess;
        private boolean processed;

        public State(ObjectOptions options, ObjectFieldOptions field, JsonProcess fieldProcess) {
            this.options = Objects.requireNonNull(options);
            this.field = Objects.requireNonNull(field);
            this.fieldProcess = Objects.requireNonNull(fieldProcess);
        }

        @Override
        public void process(String fieldName, JsonParser parser) throws IOException {
            if (!matches(fieldName, field)) {
                if (!options.allowUnknownFields()) {
                    throw new IOException(String.format("Encountered unexpected unknown field '%s'", fieldName));
                }
                parser.skipChildren();
                return;
            }
            if (processed) {
                if (field.repeatedBehavior() != RepeatedBehavior.USE_FIRST) {
                    throw new IOException(String
                            .format("Encountered repeated field '%s' and repeatedBehavior != USE_FIRST", fieldName));
                }
                parser.skipChildren();
                return;
            }
            processed = true;
            fieldProcess.process(parser);
        }

        void done() throws IOException {
            if (!processed) {
                throw new IOException(String.format("Processed object, but did not find field '%s'", field.name()));
            }
        }
    }

    static void processObjectField(JsonParser parser, ObjectOptions options, ObjectFieldOptions field,
            JsonProcess inner) throws IOException {
        final State fieldProcess = new State(options, field, inner);
        ValueProcessor.processObject(parser, fieldProcess);
        fieldProcess.done();
    }

    static void processTupleIndex(JsonParser parser, int index, JsonProcess inner) throws IOException {
        if (!parser.hasToken(JsonToken.START_ARRAY)) {
            throw new IOException("Expected START_ARRAY");
        }
        parser.nextToken();
        for (int i = 0; i < index; ++i) {
            skipElement(parser);
        }
        inner.process(parser);
        parser.nextToken();
        while (!parser.hasToken(JsonToken.END_ARRAY)) {
            skipElement(parser);
        }
    }

    static void processPath(JsonParser parser, Path path, JsonProcess inner) throws IOException {
        if (path instanceof ObjectField) {
            processObjectField(parser, ((ObjectField) path).options(), ((ObjectField) path).field(), inner);
            return;
        }
        if (path instanceof ArrayIndex) {
            processTupleIndex(parser, ((ArrayIndex) path).index(), inner);
            return;
        }
        throw new IllegalStateException();
    }

    static void processPath(JsonParser parser, List<Path> paths, JsonProcess consumer)
            throws IOException {
        if (paths.isEmpty()) {
            consumer.process(parser);
            return;
        }
        final Path path = paths.get(0);
        if (paths.size() == 1) {
            processPath(parser, path, consumer);
            return;
        }
        final List<Path> remaining = paths.subList(1, paths.size());
        processPath(parser, path, new ProcessPath(remaining, consumer));
    }

    // static JsonProcess singleFieldProcess(List<String> fieldPath, JsonProcess consumer) {
    // return fieldPath.isEmpty() ? consumer : new ProcessPath(fieldPath, consumer);
    // }

    private static void skipElement(JsonParser parser) throws IOException {
        parser.nextToken();
        parser.skipChildren();
    }

    private static final class ProcessPath implements JsonProcess {
        private final List<Path> paths;
        private final JsonProcess consumer;

        public ProcessPath(List<Path> paths, JsonProcess consumer) {
            this.paths = Objects.requireNonNull(paths);
            this.consumer = Objects.requireNonNull(consumer);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            processPath(parser, paths, consumer);
        }
    }

    static boolean matches(String fieldName, ObjectFieldOptions fieldOptions) {
        if (fieldOptions.aliases().isEmpty()) {
            return fieldOptions.caseInsensitiveMatch()
                    ? fieldOptions.name().equalsIgnoreCase(fieldName)
                    : fieldOptions.name().equals(fieldName);
        }
        final Stream<String> allNames = Stream.concat(
                Stream.of(fieldOptions.name()),
                fieldOptions.aliases().stream());
        return fieldOptions.caseInsensitiveMatch()
                ? allNames.anyMatch(fieldName::equalsIgnoreCase)
                : allNames.anyMatch(fieldName::equals);
    }
}
