//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class SourceTest {

    @Test
    void string() {
        assertThat(string(Source.of("1"))).isEqualTo("1");
    }

    @Test
    void stream() {
        assertThat(Source.stream(Source.of(List.of(Source.of("1"), Source.of("2")))).map(SourceTest::string))
                .containsExactly("1", "2");
    }

    @Test
    void nestedStream() {
        assertThat(Source.stream(Source.of(List.of(
                Source.of("1"),
                Source.of(List.of(Source.of("2"), Source.of("3")))))).map(SourceTest::string))
                .containsExactly("1", "2", "3");
    }

    private static String string(Source source) {
        return source.walk(StringVisitor.INSTANCE);
    }

    enum StringVisitor implements Source.Visitor<String> {
        INSTANCE;

        @Override
        public String visit(String content) {
            return content;
        }

        @Override
        public String visit(ByteBuffer buffer) {
            return null;
        }

        @Override
        public String visit(CharBuffer buffer) {
            return null;
        }

        @Override
        public String visit(File file) {
            return null;
        }

        @Override
        public String visit(Path path) {
            return null;
        }

        @Override
        public String visit(InputStream inputStream) {
            return null;
        }

        @Override
        public String visit(URL url) {
            return null;
        }

        @Override
        public String visit(Collection<Source> sources) {
            return null;
        }
    }
}
