//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadFeature;
import io.deephaven.json.Source;
import io.deephaven.json.Source.Visitor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Objects;

final class JacksonSource implements Visitor<JsonParser> {
    public static JsonParser of(JsonFactory factory, Source source) throws IOException {
        try {
            return source.walk(new JacksonSource(factory));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }

    public static JsonParser of(JsonFactory factory, String content) throws IOException {
        return factory.createParser(content);
    }

    public static JsonParser of(JsonFactory factory, File file) throws IOException {
        return factory.createParser(file);
    }

    // todo: suggest jackson build this in
    public static JsonParser of(JsonFactory factory, Path path) throws IOException {
        if (FileSystems.getDefault() == path.getFileSystem()) {
            return of(factory, path.toFile());
        }
        if (!factory.isEnabled(StreamReadFeature.AUTO_CLOSE_SOURCE)) {
            throw new RuntimeException(String.format("Unable to create Path-based parser when '%s' is not enabled",
                    StreamReadFeature.AUTO_CLOSE_SOURCE));
        }
        // jackson buffers internally
        return factory.createParser(Files.newInputStream(path));
    }

    public static JsonParser of(JsonFactory factory, InputStream inputStream) throws IOException {
        return factory.createParser(inputStream);
    }

    public static JsonParser of(JsonFactory factory, URL url) throws IOException {
        return factory.createParser(url);
    }

    // todo: suggest jackson build this in
    public static JsonParser of(JsonFactory factory, ByteBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            return factory.createParser(buffer.array(), buffer.position(), buffer.remaining());
        }
        return of(factory, ByteBufferInputStream.of(buffer));
    }

    // todo: suggest jackson build this in
    public static JsonParser of(JsonFactory factory, CharBuffer buffer) throws IOException {
        if (buffer.hasArray()) {
            return factory.createParser(buffer.array(), buffer.position(), buffer.remaining());
        }
        // todo: we could build CharBufferReader. Surprised it's not build into JDK.
        throw new RuntimeException("todo");
    }

    private final JsonFactory factory;

    public JacksonSource(JsonFactory factory) {
        this.factory = Objects.requireNonNull(factory);
    }

    @Override
    public JsonParser visit(String content) {
        try {
            return of(factory, content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(ByteBuffer buffer) {
        try {
            return of(factory, buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(CharBuffer buffer) {
        try {
            return of(factory, buffer);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(File file) {
        try {
            return of(factory, file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(Path path) {
        try {
            return of(factory, path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(InputStream inputStream) {
        try {
            return of(factory, inputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(URL url) {
        try {
            return of(factory, url);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public JsonParser visit(Collection<Source> sources) {
        throw new IllegalStateException("Expected single source");
    }
}
