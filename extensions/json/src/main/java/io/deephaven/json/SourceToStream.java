//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Stream;

enum SourceToStream implements Source.Visitor<Stream<Source>> {
    INSTANCE;

    @Override
    public Stream<Source> visit(String content) {
        return Stream.of(Source.of(content));
    }

    @Override
    public Stream<Source> visit(ByteBuffer buffer) {
        return Stream.of(Source.of(buffer));
    }

    @Override
    public Stream<Source> visit(CharBuffer buffer) {
        return Stream.of(Source.of(buffer));
    }

    @Override
    public Stream<Source> visit(File file) {
        return Stream.of(Source.of(file));
    }

    @Override
    public Stream<Source> visit(Path path) {
        return Stream.of(Source.of(path));
    }

    @Override
    public Stream<Source> visit(InputStream inputStream) {
        return Stream.of(Source.of(inputStream));
    }

    @Override
    public Stream<Source> visit(URL url) {
        return Stream.of(Source.of(url));
    }

    @Override
    public Stream<Source> visit(Collection<Source> sources) {
        return sources.stream().flatMap(Source::stream);
    }
}
