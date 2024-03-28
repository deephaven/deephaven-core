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
import java.util.Objects;
import java.util.stream.Stream;

public interface Source {

    static Source of(String content) {
        Objects.requireNonNull(content);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(content);
            }
        };
    }

    static Source of(ByteBuffer content) {
        Objects.requireNonNull(content);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(content);
            }
        };
    }

    static Source of(CharBuffer content) {
        Objects.requireNonNull(content);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(content);
            }
        };
    }

    static Source of(File file) {
        Objects.requireNonNull(file);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(file);
            }
        };
    }

    static Source of(Path path) {
        Objects.requireNonNull(path);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(path);
            }
        };
    }

    static Source of(InputStream inputStream) {
        Objects.requireNonNull(inputStream);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(inputStream);
            }
        };
    }

    static Source of(URL url) {
        Objects.requireNonNull(url);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(url);
            }
        };
    }

    static Source of(Collection<Source> sources) {
        Objects.requireNonNull(sources);
        return new Source() {
            @Override
            public <T> T walk(Visitor<T> visitor) {
                return visitor.visit(sources);
            }
        };
    }

    static Stream<Source> stream(Source source) {
        return source.walk(SourceToStream.INSTANCE);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {

        T visit(String content);

        T visit(ByteBuffer buffer);

        T visit(CharBuffer buffer);

        T visit(File file);

        T visit(Path path);

        T visit(InputStream inputStream);

        T visit(URL url);

        T visit(Collection<Source> sources);
    }
}
