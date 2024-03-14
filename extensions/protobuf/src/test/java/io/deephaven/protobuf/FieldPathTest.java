//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import io.deephaven.function.ToBooleanFunction;
import io.deephaven.protobuf.test.FieldPathTesting;
import io.deephaven.protobuf.test.FieldPathTesting.Bar;
import io.deephaven.protobuf.test.FieldPathTesting.Baz;
import io.deephaven.protobuf.test.FieldPathTesting.Foo;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class FieldPathTest {

    private static final FieldDescriptor FOO = FieldPathTesting.getDescriptor().findFieldByName("foo");
    private static final FieldDescriptor BAR = Foo.getDescriptor().findFieldByName("bar");
    private static final FieldDescriptor BAZ = Bar.getDescriptor().findFieldByName("baz");
    private static final FieldDescriptor ZIP = Baz.getDescriptor().findFieldByName("zip");
    private static final FieldDescriptor ZAP = Foo.getDescriptor().findFieldByName("zap");
    private static final FieldDescriptor ZOOM = Foo.getDescriptor().findFieldByName("zoom");

    @Test
    void namePath() {
        assertThat(FieldPath.of(FOO, ZOOM).namePath()).containsExactly("foo", "zoom");
    }

    @Test
    void numberPath() {
        assertThat(FieldPath.of(FOO, ZOOM).numberPath()).isEqualTo(FieldNumberPath.of(1, 3));
    }

    @Test
    void namePathStartsWith() {
        assertThat(FieldPath.of(FOO, ZOOM).startsWith(List.of())).isTrue();
        assertThat(FieldPath.of(FOO, ZOOM).startsWith(List.of("foo"))).isTrue();
        assertThat(FieldPath.of(FOO, ZOOM).startsWith(List.of("foo", "zoom"))).isTrue();
        assertThat(FieldPath.of(FOO, ZOOM).startsWith(List.of("foo", "zoom", "oops"))).isFalse();
        assertThat(FieldPath.of(FOO, ZOOM).startsWith(List.of("foo", "bar"))).isFalse();
        assertThat(FieldPath.of(FOO, ZOOM).startsWith(List.of("bar"))).isFalse();
    }

    @Test
    void namePathOtherStartsWithThis() {
        assertThat(FieldPath.of(FOO, ZOOM).otherStartsWithThis(List.of())).isFalse();
        assertThat(FieldPath.of(FOO, ZOOM).otherStartsWithThis(List.of("foo"))).isFalse();
        assertThat(FieldPath.of(FOO, ZOOM).otherStartsWithThis(List.of("foo", "zoom"))).isTrue();
        assertThat(FieldPath.of(FOO, ZOOM).otherStartsWithThis(List.of("foo", "zoom", "oops"))).isTrue();
        assertThat(FieldPath.of(FOO, ZOOM).otherStartsWithThis(List.of("foo", "bar"))).isFalse();
        assertThat(FieldPath.of(FOO, ZOOM).otherStartsWithThis(List.of("bar"))).isFalse();
    }

    @Test
    void fooBarMatches() {
        for (final String simplePath : List.of("/foo/bar", "foo/bar")) {
            final ToBooleanFunction<FieldPath> matcher = FieldPath.matches(simplePath);
            assertThat(matcher.test(FieldPath.empty())).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO))).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO, BAR))).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO, BAR, BAZ))).isFalse();
            assertThat(matcher.test(FieldPath.of(FOO, BAR, BAZ, ZIP))).isFalse();
            assertThat(matcher.test(FieldPath.of(FOO, ZAP))).isFalse();
            assertThat(matcher.test(FieldPath.of(FOO, ZOOM))).isFalse();
        }
    }

    @Test
    void fooBarStarMatches() {
        for (final String simplePath : List.of("/foo/bar/*", "foo/bar/*")) {
            final ToBooleanFunction<FieldPath> matcher = FieldPath.matches(simplePath);
            assertThat(matcher.test(FieldPath.empty())).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO))).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO, BAR))).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO, BAR, BAZ))).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO, BAR, BAZ, ZIP))).isTrue();
            assertThat(matcher.test(FieldPath.of(FOO, ZAP))).isFalse();
            assertThat(matcher.test(FieldPath.of(FOO, ZOOM))).isFalse();
        }
    }

    @Test
    void zipZoomAnyMatches() {
        final ToBooleanFunction<FieldPath> matcher = FieldPath.anyMatches(List.of("/foo/bar/baz/zip", "/foo/zoom"));
        assertThat(matcher.test(FieldPath.empty())).isTrue();
        assertThat(matcher.test(FieldPath.of(FOO))).isTrue();
        assertThat(matcher.test(FieldPath.of(FOO, BAR))).isTrue();
        assertThat(matcher.test(FieldPath.of(FOO, BAR, BAZ))).isTrue();
        assertThat(matcher.test(FieldPath.of(FOO, BAR, BAZ, ZIP))).isTrue();
        assertThat(matcher.test(FieldPath.of(FOO, ZAP))).isFalse();
        assertThat(matcher.test(FieldPath.of(FOO, ZOOM))).isTrue();
    }
}
