//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkEquals;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.json.Source.Visitor;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.util.BooleanUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.assertj.core.api.Assertions.assertThat;

public class JsonStreamPublisherTest {

    public static Source empty() {
        return Source.of("");
    }

    public static Source nullToken() {
        return Source.of("null");
    }

    public static Source objEmpty() {
        return Source.of("{}");
    }

    public static Source arrayEmpty() {
        return Source.of("[]");
    }

    public static Source _int(int x) {
        return Source.of(Integer.toString(x));
    }

    public static Source objNameAge(String name, int age) {
        // we are assuming name is simple here
        return Source.of(String.format("{\"name\":\"%s\", \"age\":%d}", name, age));
    }

    public static Source objName(String name) {
        // we are assuming name is simple here
        return Source.of(String.format("{\"name\":\"%s\"}", name));
    }

    public static Source objAge(int age) {
        return Source.of(String.format("{\"age\":%d}", age));
    }

    public static Source array(Source... sources) {
        return Source.of(Arrays
                .stream(sources)
                .map(JsonStreamPublisherTest::string)
                .collect(Collectors.joining(",", "[", "]")));
    }

    public static Source newlineDelimited(Source... sources) {
        return Source.of(Arrays
                .stream(sources)
                .map(JsonStreamPublisherTest::string)
                .collect(Collectors.joining("\n")));
    }

    public static ObjectOptions nameAgeOptions() {
        return ObjectOptions.builder()
                .putFields("name", StringOptions.standard())
                .putFields("age", IntOptions.standard())
                .build();
    }

    @Test
    void missingPrimitive() {
        final JsonStreamPublisher publisher = publisher(IntOptions.standard(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, empty());
            recorder.flushPublisher();
            recorder.assertEquals(IntChunk.chunkWrap(new int[] {NULL_INT}));
            recorder.clear();
        }
    }

    @Test
    void emptyObject() {
        final JsonStreamPublisher publisher = publisher(nameAgeOptions(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, objEmpty());
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {null}),
                    IntChunk.chunkWrap(new int[] {NULL_INT}));
            recorder.clear();
        }
    }

    @Test
    void emptyArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(nameAgeOptions()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, arrayEmpty());
            recorder.flushPublisher();
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void emptyObjectKv() {
        final JsonStreamPublisher publisher =
                publisher(ObjectKvOptions.standard(StringOptions.standard(), IntOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, objEmpty());
            recorder.flushPublisher();
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void singlePrimitive() {
        final JsonStreamPublisher publisher = publisher(IntOptions.standard(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, _int(42), nullToken(), _int(43));
            recorder.flushPublisher();
            recorder.assertEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    @Test
    void singleObject() {
        final JsonStreamPublisher publisher = publisher(nameAgeOptions(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher,
                    objNameAge("foo", 42),
                    nullToken(),
                    objNameAge("bar", 43),
                    objName("baz"),
                    objAge(44),
                    objEmpty());
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"foo", null, "bar", "baz", null, null}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, NULL_INT, 44, NULL_INT}));
            recorder.clear();
        }
    }

    @Test
    void singlePrimitiveArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(IntOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [42, null, 43]
            directExecute(publisher, array(_int(42), nullToken(), _int(43)));
            recorder.flushPublisher();
            recorder.assertEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    @Test
    void singleBooleanArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(BoolOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [true, null, false]
            directExecute(publisher, Source.of("[true, null, false]"));
            recorder.flushPublisher();
            recorder.assertEquals(ByteChunk.chunkWrap(new byte[] {
                    BooleanUtils.TRUE_BOOLEAN_AS_BYTE,
                    BooleanUtils.NULL_BOOLEAN_AS_BYTE,
                    BooleanUtils.FALSE_BOOLEAN_AS_BYTE}));
            recorder.clear();
        }
    }

    @Test
    void singleObjectArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(nameAgeOptions()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [{"name": "foo", "age":42}, null, {"name": "bar", "age": 43}, {"name": "baz"}, {"age": 44}, {}]
            directExecute(publisher, array(
                    objNameAge("foo", 42),
                    nullToken(),
                    objNameAge("bar", 43),
                    objName("baz"),
                    objAge(44),
                    objEmpty()));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"foo", null, "bar", "baz", null, null}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, NULL_INT, 44, NULL_INT}));
            recorder.clear();
        }
    }

    // This is harder to implement than it looks like; right now, we expect chunks to be handed off at the same time and
    // do it in a streaming fashion. But in the case where we have a very large grouped array, we'd need to buffer all
    // of the first column.
    //
    // {
    // "prices": [1.0, 2.0, ..., 1000000.0],
    // "sizes": [1, 2, ..., 1000000]
    // }
    //
    // whereas with the logical equivalent (from data perspective):
    //
    // [ { "price": 1.0, "size": 1 }, { "price": 2.0, "size": 2 }, ..., { "price": 1000000.0, "size": 1000000 } ]
    //
    // can process this in streaming fashion.
    @Disabled("feature not implemented")
    @Test
    void singleObjectFieldArrayGroup() throws IOException {
        final JsonStreamPublisher publisher = publisher(ObjectOptions.builder()
                .addFields(ObjectFieldOptions.builder()
                        .name("names")
                        .options(StringOptions.standard().array())
                        .arrayGroup("names_and_ages")
                        .build())
                .addFields(ObjectFieldOptions.builder()
                        .name("ages")
                        .options(IntOptions.standard().array())
                        .arrayGroup("names_and_ages")
                        .build())
                .build(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // { "names": ["foo", null, "bar", "baz", null, null], "ages": [42, null, 43, null, 44, null] }
            directExecute(publisher, Source.of(
                    "{ \"names\": [\"foo\", null, \"bar\", \"baz\", null, null], \"ages\": [42, null, 43, null, 44, null] }"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"foo", null, "bar", "baz", null, null}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, NULL_INT, 44, NULL_INT}));
            recorder.clear();
        }
    }

    @Test
    void singlePrimitiveKv() {
        final JsonStreamPublisher publisher =
                publisher(ObjectKvOptions.standard(StringOptions.standard(), IntOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // { "A": 42, "B": null, "C": 43}
            directExecute(publisher, Source.of("{ \"A\": 42, \"B\": null, \"C\": 43}"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"A", "B", "C"}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    @Test
    void singleObjectKv() {
        final JsonStreamPublisher publisher =
                publisher(ObjectKvOptions.standard(StringOptions.standard(), nameAgeOptions()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // { "a": {"name": "foo", "age":42}, "b": null, "c": {"name": "bar", "age": 43}, "d": {"name": "baz"}, "e":
            // {"age": 44}, "f": {} }
            directExecute(publisher, Source.of(
                    "{ \"a\": {\"name\": \"foo\", \"age\":42}, \"b\": null, \"c\": {\"name\": \"bar\", \"age\": 43}, \"d\": {\"name\": \"baz\"}, \"e\": {\"age\": 44}, \"f\": {} }"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"a", "b", "c", "d", "e", "f"}),
                    ObjectChunk.chunkWrap(new String[] {"foo", null, "bar", "baz", null, null}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, NULL_INT, 44, NULL_INT}));
            recorder.clear();
        }
    }

    @Test
    void multiValuePrimitive() {
        final JsonStreamPublisher publisher = publisher(IntOptions.standard(), true);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // 42 null 43
            directExecute(publisher, newlineDelimited(_int(42), nullToken(), _int(43)));
            recorder.flushPublisher();
            recorder.assertEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    @Test
    void multiValueObject() {
        final JsonStreamPublisher publisher = publisher(nameAgeOptions(), true);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // {"name": "foo", "age": 42} null {"name": "bar", "age": 43} {"name": "baz"} {"age": 44} {}
            directExecute(publisher, newlineDelimited(
                    objNameAge("foo", 42),
                    nullToken(),
                    objNameAge("bar", 43),
                    objName("baz"),
                    objAge(44),
                    objEmpty()));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"foo", null, "bar", "baz", null, null}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, NULL_INT, 44, NULL_INT}));
            recorder.clear();
        }
    }

    @Test
    void multiValuePrimitiveArray() {
        final JsonStreamPublisher publisher = publisher(ArrayOptions.strict(IntOptions.standard()), true);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [42] [] [null, 43] [44]
            directExecute(publisher, newlineDelimited(
                    array(_int(42)),
                    arrayEmpty(),
                    array(nullToken(), _int(43)),
                    array(_int(44))));
            recorder.flushPublisher();
            recorder.assertEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, 44}));
            recorder.clear();
        }
    }

    @Test
    void multiValuePrimitiveKv() {
        final JsonStreamPublisher publisher =
                publisher(ObjectKvOptions.standard(StringOptions.standard(), IntOptions.standard()), true);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // {"a": 42} {} {"b": null, "c": 43} {"d": 44}
            directExecute(publisher, Source.of("{\"a\": 42} {} {\"b\": null, \"c\": 43} {\"d\": 44}"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"a", "b", "c", "d"}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43, 44}));
            recorder.clear();
        }
    }

    @Test
    void nestedPrimitiveArray() {
        final JsonStreamPublisher publisher = publisher(ObjectOptions.builder()
                .putFields("a", ObjectOptions.builder()
                        .putFields("b", ObjectOptions.builder()
                                .putFields("c", ArrayOptions.strict(IntOptions.standard()))
                                .build())
                        .build())
                .build(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, Source.of("{\"a\": {\"b\": {\"c\": [42, null, 43]}}}"));
            // Ensure extra fields are ignored
            directExecute(publisher, Source.of(
                    "{\"a\": {\"skip\": {}, \"b\": {\"skip\": {}, \"c\": [42, null, 43], \"skip\": {}}}, \"skip\": {}}"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void nestedPrimitiveKv() {
        final JsonStreamPublisher publisher = publisher(ObjectOptions.builder()
                .putFields("a", ObjectOptions.builder()
                        .putFields("b", ObjectOptions.builder()
                                .putFields("c",
                                        ObjectKvOptions.standard(StringOptions.standard(), IntOptions.standard()))
                                .build())
                        .build())
                .build(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // { "a": {"b": {"c": {"D": 42, "E": null, "F": 43}}}}
            directExecute(publisher, Source.of("{ \"a\": {\"b\": {\"c\": {\"D\": 42, \"E\": null, \"F\": 43}}}}"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"D", "E", "F"}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    @Test
    void nestedTuplePrimitiveArray() {
        final JsonStreamPublisher publisher = publisher(TupleOptions.of(TupleOptions.of(TupleOptions.of(
                ArrayOptions.strict(IntOptions.standard())))), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, Source.of("[[[[42, null, 43]]]]"));
            // Ensure skipping over extra elements
            directExecute(publisher, Source.of("[[[[42, null, 43], {}], {}], {}]"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void nestedTuplePrimitiveKv() {
        final JsonStreamPublisher publisher = publisher(TupleOptions.of(TupleOptions.of(TupleOptions.of(
                ObjectKvOptions.strict(StringOptions.standard(), IntOptions.standard())))), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [[[{"a": 42, "b": null, "c": 43}]]]
            directExecute(publisher, Source.of("[[[{\"a\": 42, \"b\": null, \"c\": 43}]]]"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"a", "b", "c"}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    // @Disabled("feature not implemented")
    @Test
    void boundaryCollapse() {
        // TODO: get this working
        // TODO: have some options that will collect an ID so the user _could_ join on it?
        // [ { "ref": [ { "name": "Foo", "age": 42 }, { "name": "Bar", "age": 43 } ] }, { "ref": [ { "name": "Baz",
        // "age": 44 } ] } ]
        final JsonStreamPublisher publisher = publisher(nameAgeOptions().array().field("ref").array(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, Source.of(
                    "[ { \"ref\": [ { \"name\": \"Foo\", \"age\": 42 }, { \"name\": \"Bar\", \"age\": 43 } ] }, { \"ref\": [ { \"name\": \"Baz\", \"age\": 44 } ] } ]"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"Foo", "Bar", "Baz"}),
                    IntChunk.chunkWrap(new int[] {42, 43, 44}));
            recorder.clear();
        }
    }

    @Test
    void boundaryPreserve() {
        // Same data as above, but we have a barrier that stops collapsing the single path
        // [ { "ref": [ { "name": "Foo", "age": 42 }, { "name": "Bar", "age": 43 } ] }, { "ref": [ { "name": "Baz",
        // "age": 44 } ] } ]
        final JsonStreamPublisher publisher = publisher(ObjectOptions.builder()
                .putFields("ref", nameAgeOptions().array())
                .putFields("barrier", SkipOptions.lenient())
                .build()
                .array(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, Source.of(
                    "[ { \"ref\": [ { \"name\": \"Foo\", \"age\": 42 }, { \"name\": \"Bar\", \"age\": 43 } ] }, { \"ref\": [ { \"name\": \"Baz\", \"age\": 44 } ] } ]"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new Object[] {new String[] {"Foo", "Bar"}, new String[] {"Baz"}}),
                    ObjectChunk.chunkWrap(new Object[] {new int[] {42, 43}, new int[] {44}}));
            recorder.clear();
        }
    }

    @Test
    void tupleNestedRefArray() {
        final JsonStreamPublisher publisher = publisher(TupleOptions.of(ObjectOptions.builder()
                .putFields("ref", ArrayOptions.strict(IntOptions.standard()))
                .build()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, Source.of("[{\"ref\": [42, null, 43]}]"));
            // Ensure skipping over extra fields and elements
            directExecute(publisher, Source.of("[{\"foo\": {}, \"ref\": [42, null, 43], \"bar\": {}}, {}]"));
            recorder.flushPublisher();
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    @Test
    void tupleNestedRefKv() {
        final JsonStreamPublisher publisher = publisher(TupleOptions.of(ObjectOptions.builder()
                .putFields("ref", ObjectKvOptions.strict(StringOptions.standard(), IntOptions.standard()))
                .build()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [{"ref": {"a": 42, "b": null, "c": 43}}]
            directExecute(publisher, Source.of("[{\"ref\": {\"a\": 42, \"b\": null, \"c\": 43}}]"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"a", "b", "c"}),
                    IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.clear();
        }
    }

    @Test
    void booleanPrimitiveNotUnrolled() {
        final JsonStreamPublisher publisher =
                publisher(TupleOptions.of(StringOptions.standard(), BoolOptions.standard()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // ["foo", true]
            directExecute(publisher, Source.of("[\"foo\", true]"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"foo"}),
                    ByteChunk.chunkWrap(new byte[] {BooleanUtils.TRUE_BOOLEAN_AS_BYTE}));
            recorder.clear();
        }
    }

    @Test
    void booleanArrayUnrolled() {
        final JsonStreamPublisher publisher = publisher(BoolOptions.standard().array(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // [true, false, null]
            directExecute(publisher, Source.of("[true, null, false]"));
            recorder.flushPublisher();
            recorder.assertEquals(ByteChunk.chunkWrap(new byte[] {
                    BooleanUtils.TRUE_BOOLEAN_AS_BYTE,
                    BooleanUtils.NULL_BOOLEAN_AS_BYTE,
                    BooleanUtils.FALSE_BOOLEAN_AS_BYTE}));
            recorder.clear();
        }
    }


    @Test
    void booleanArrayNotUnrolled() {
        final JsonStreamPublisher publisher =
                publisher(TupleOptions.of(StringOptions.standard(), BoolOptions.standard().array()), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            // ["foo", [true, null, false]]
            directExecute(publisher, Source.of("[\"foo\", [true, null, false]]"));
            recorder.flushPublisher();
            recorder.assertEquals(
                    ObjectChunk.chunkWrap(new String[] {"foo"}),
                    ObjectChunk.chunkWrap(new Boolean[][] {new Boolean[] {true, null, false}}));
            recorder.clear();
        }
    }

    @Test
    void separateExecutes() {
        final JsonStreamPublisher publisher = publisher(IntOptions.standard(), false);
        try (final StreamConsumerRecorder recorder = new StreamConsumerRecorder(publisher)) {
            publisher.register(recorder);
            directExecute(publisher, Source.of("42"), nullToken(), Source.of("43"));
            directExecute(publisher, Source.of("1"), Source.of("-2"));
            recorder.flushPublisher();
            // Separate executes are submitted to consumer separately
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {42, NULL_INT, 43}));
            recorder.assertNextChunksEquals(IntChunk.chunkWrap(new int[] {1, -2}));
            recorder.assertEmpty();
            recorder.clear();
        }
    }

    private static JsonStreamPublisher publisher(ValueOptions options, boolean multiValueSupport) {
        return JsonStreamPublisherOptions.builder()
                .options(options)
                .multiValueSupport(multiValueSupport)
                .chunkSize(1024)
                .build()
                .execute();
    }

    private static void directExecute(JsonStreamPublisher publisher, Source... sources) {
        publisher.execute(Runnable::run, new LinkedList<>(Arrays.asList(sources)));
    }

    // todo: consolidate between this and io.deephaven.kafka.v2.PublishersOptionsBase.StreamConsumerRecorder
    private static class StreamConsumerRecorder implements StreamConsumer, Closeable {
        final StreamPublisher publisher;
        final List<WritableChunk<Values>[]> accepted = new ArrayList<>();
        final List<Throwable> failures = new ArrayList<>();
        // private final LongAdder rowCount = new LongAdder();

        private StreamConsumerRecorder(StreamPublisher publisher) {
            this.publisher = Objects.requireNonNull(publisher);
        }

        public void flushPublisher() {
            publisher.flush();
        }

        public synchronized void clear() {
            if (!failures.isEmpty()) {
                throw new IllegalStateException();
            }
            accepted.clear();
        }

        @Override
        public synchronized void accept(@NotNull WritableChunk<Values>... data) {
            // rowCount.add(data[0].size());
            accepted.add(data);
        }

        @Override
        public synchronized void accept(@NotNull Collection<WritableChunk<Values>[]> data) {
            // rowCount.add(data.stream().map(x -> x[0]).mapToInt(Chunk::size).sum());
            accepted.addAll(data);
        }

        @Override
        public synchronized void acceptFailure(@NotNull Throwable cause) {
            failures.add(cause);
        }

        @Override
        public void close() {
            publisher.shutdown();
        }

        public synchronized Chunk<?> singleValue() {
            assertThat(failures).isEmpty();
            assertThat(accepted).hasSize(1);
            assertThat(accepted.get(0)).hasSize(1);
            return accepted.get(0)[0];
        }

        public synchronized void assertEquals(Chunk<?>... expectedChunks) {
            assertThat(failures).isEmpty();
            assertThat(accepted).hasSize(1);
            assertNextChunksEquals(expectedChunks);
        }

        public synchronized void assertEmpty() {
            assertThat(failures).isEmpty();
            assertThat(accepted).isEmpty();
        }

        public synchronized void assertNextChunksEquals(Chunk<?>... expectedChunks) {
            assertThat(failures).isEmpty();
            final Iterator<WritableChunk<Values>[]> it = accepted.iterator();
            assertThat(it).hasNext();
            final WritableChunk<Values>[] chunks = it.next();
            assertThat(chunks).hasSize(expectedChunks.length);
            for (int i = 0; i < chunks.length; ++i) {
                assertThat(chunks[i]).usingComparator(FAKE_COMPARE_FOR_EQUALS).isEqualTo(expectedChunks[i]);
            }
            it.remove();
        }
    }

    private static final Comparator<Chunk<?>> FAKE_COMPARE_FOR_EQUALS =
            JsonStreamPublisherTest::fakeCompareForEquals;

    private static int fakeCompareForEquals(Chunk<?> x, Chunk<?> y) {
        return ChunkEquals.equals(x, y) ? 0 : 1;
    }

    private static String string(Source source) {
        return source.walk(StringContent.INSTANCE);
    }

    enum StringContent implements Visitor<String> {
        INSTANCE;

        @Override
        public String visit(String content) {
            return content;
        }

        @Override
        public String visit(ByteBuffer buffer) {
            throw new IllegalStateException();
        }

        @Override
        public String visit(CharBuffer buffer) {
            throw new IllegalStateException();
        }

        @Override
        public String visit(File file) {
            throw new IllegalStateException();
        }

        @Override
        public String visit(Path path) {
            throw new IllegalStateException();
        }

        @Override
        public String visit(InputStream inputStream) {
            throw new IllegalStateException();
        }

        @Override
        public String visit(URL url) {
            throw new IllegalStateException();
        }

        @Override
        public String visit(Collection<Source> sources) {
            throw new IllegalStateException();
        }
    }
}
