//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.api.util.NameValidator;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.json.AnyOptions;
import io.deephaven.json.ArrayOptions;
import io.deephaven.json.BigDecimalOptions;
import io.deephaven.json.BigIntegerOptions;
import io.deephaven.json.BoolOptions;
import io.deephaven.json.ByteOptions;
import io.deephaven.json.CharOptions;
import io.deephaven.json.DoubleOptions;
import io.deephaven.json.FloatOptions;
import io.deephaven.json.InstantNumberOptions;
import io.deephaven.json.InstantOptions;
import io.deephaven.json.IntOptions;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectFieldOptions;
import io.deephaven.json.ObjectKvOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ShortOptions;
import io.deephaven.json.SkipOptions;
import io.deephaven.json.StringOptions;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.TypedObjectOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.processor.NamedObjectProcessor;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class Mixin<T extends ValueOptions> implements JacksonProcessors {

    static final Function<List<String>, String> TO_COLUMN_NAME = Mixin::toColumnName;

    static Mixin<?> of(ValueOptions options, JsonFactory factory) {
        return options.walk(new MixinImpl(factory));
    }

    private final JsonFactory factory; // todo: probably shouldn't take factory at this level, work w/ Source instead
    final T options;

    Mixin(JsonFactory factory, T options) {
        this.factory = Objects.requireNonNull(factory);
        this.options = Objects.requireNonNull(options);
    }

    public final T options() {
        return options;
    }

    public static String toColumnName(List<String> path) {
        return path.isEmpty() ? "Value" : String.join("_", path);
    }

    final Mixin<?> mixin(ValueOptions options) {
        return of(options, factory);
    }

    abstract ValueProcessor processor(String context, List<WritableChunk<?>> out);

    abstract RepeaterProcessor repeaterProcessor(boolean allowMissing, boolean allowNull, List<WritableChunk<?>> out);

    abstract int numColumns();

    abstract Stream<List<String>> paths();

    @SuppressWarnings("unchecked")
    @Override
    public final <X> ObjectProcessor<? super X> processor(Class<X> inputType) {
        if (String.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super X>) stringProcessor();
        }
        if (byte[].class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super X>) bytesProcessor();
        }
        if (char[].class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super X>) charsProcessor();
        }
        if (File.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super X>) fileProcessor();
        }
        if (URL.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super X>) urlProcessor();
        }
        if (ByteBuffer.class.isAssignableFrom(inputType)) {
            return (ObjectProcessor<? super X>) byteBufferProcessor();
        }
        throw new IllegalArgumentException("Unable to create JSON processor from type " + inputType.getName());
    }

    @Override
    public final <X> NamedObjectProcessor<? super X> named(Class<X> inputType) {
        return NamedObjectProcessor.of(processor(inputType), names(TO_COLUMN_NAME));
    }

    @Override
    public final List<String> names(Function<List<String>, String> f) {
        return Arrays.asList(NameValidator.legalizeColumnNames(paths().map(f).toArray(String[]::new), true));
    }

    @Override
    public final ObjectProcessor<String> stringProcessor() {
        return new StringIn();
    }

    @Override
    public final ObjectProcessor<byte[]> bytesProcessor() {
        return new BytesIn();
    }

    @Override
    public final ObjectProcessor<char[]> charsProcessor() {
        return new CharsIn();
    }

    @Override
    public final ObjectProcessor<File> fileProcessor() {
        return new FileIn();
    }

    @Override
    public final ObjectProcessor<URL> urlProcessor() {
        return new URLIn();
    }

    @Override
    public final ObjectProcessor<ByteBuffer> byteBufferProcessor() {
        return new ByteBufferIn();
    }

    static List<String> prefixWith(String prefix, List<String> path) {
        return Stream.concat(Stream.of(prefix), path.stream()).collect(Collectors.toList());
    }

    Stream<List<String>> prefixWithKeys(Collection<ObjectFieldOptions> fields) {
        final List<Stream<List<String>>> paths = new ArrayList<>(fields.size());
        for (ObjectFieldOptions field : fields) {
            final Stream<List<String>> prefixedPaths =
                    mixin(field.options()).paths().map(x -> prefixWith(field.name(), x));
            paths.add(prefixedPaths);
        }
        return paths.stream().flatMap(Function.identity());
    }

    private class StringIn extends ObjectProcessorJsonValue<String> {
        @Override
        protected JsonParser createParser(JsonFactory factory, String in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class BytesIn extends ObjectProcessorJsonValue<byte[]> {
        @Override
        protected JsonParser createParser(JsonFactory factory, byte[] in) throws IOException {
            return JacksonSource.of(factory, ByteBuffer.wrap(in));
        }
    }

    private class ByteBufferIn extends ObjectProcessorJsonValue<ByteBuffer> {
        @Override
        protected JsonParser createParser(JsonFactory factory, ByteBuffer in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class CharsIn extends ObjectProcessorJsonValue<char[]> {
        @Override
        protected JsonParser createParser(JsonFactory factory, char[] in) throws IOException {
            return JacksonSource.of(factory, CharBuffer.wrap(in));
        }
    }

    private class FileIn extends ObjectProcessorJsonValue<File> {
        @Override
        protected JsonParser createParser(JsonFactory factory, File in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private class URLIn extends ObjectProcessorJsonValue<URL> {
        @Override
        protected JsonParser createParser(JsonFactory factory, URL in) throws IOException {
            return JacksonSource.of(factory, in);
        }
    }

    private abstract class ObjectProcessorJsonValue<X> implements ObjectProcessor<X> {

        protected abstract JsonParser createParser(JsonFactory factory, X in) throws IOException;

        @Override
        public final int size() {
            return numColumns();
        }

        @Override
        public final List<Type<?>> outputTypes() {
            return Mixin.this.outputTypes().collect(Collectors.toList());
        }

        @Override
        public final void processAll(ObjectChunk<? extends X, ?> in, List<WritableChunk<?>> out) {
            try {
                processAllImpl(in, out);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void processAllImpl(ObjectChunk<? extends X, ?> in, List<WritableChunk<?>> out) throws IOException {
            final ValueProcessor valueProcessor = processor("<root>", out);
            for (int i = 0; i < in.size(); ++i) {
                try (final JsonParser parser = createParser(factory, in.get(i))) {
                    ValueProcessor.processFullJson(valueProcessor, parser);
                }
            }
        }
    }

    private static class MixinImpl implements ValueOptions.Visitor<Mixin<?>> {
        private final JsonFactory factory;

        public MixinImpl(JsonFactory factory) {
            this.factory = factory;
        }

        @Override
        public StringMixin visit(StringOptions _string) {
            return new StringMixin(_string, factory);
        }

        @Override
        public Mixin<?> visit(BoolOptions _bool) {
            return new BoolMixin(_bool, factory);
        }

        @Override
        public Mixin<?> visit(ByteOptions _byte) {
            return new ByteMixin(_byte, factory);
        }

        @Override
        public Mixin<?> visit(CharOptions _char) {
            return new CharMixin(_char, factory);
        }

        @Override
        public Mixin<?> visit(ShortOptions _short) {
            return new ShortMixin(_short, factory);
        }

        @Override
        public IntMixin visit(IntOptions _int) {
            return new IntMixin(_int, factory);
        }

        @Override
        public LongMixin visit(LongOptions _long) {
            return new LongMixin(_long, factory);
        }

        @Override
        public FloatMixin visit(FloatOptions _float) {
            return new FloatMixin(_float, factory);
        }

        @Override
        public DoubleMixin visit(DoubleOptions _double) {
            return new DoubleMixin(_double, factory);
        }

        @Override
        public ObjectMixin visit(ObjectOptions object) {
            return new ObjectMixin(object, factory);
        }

        @Override
        public Mixin<?> visit(ObjectKvOptions objectKv) {
            return new ObjectKvMixin(objectKv, factory);
        }

        @Override
        public InstantMixin visit(InstantOptions instant) {
            return new InstantMixin(instant, factory);
        }

        @Override
        public InstantNumberMixin visit(InstantNumberOptions instantNumber) {
            return new InstantNumberMixin(instantNumber, factory);
        }

        @Override
        public BigIntegerMixin visit(BigIntegerOptions bigInteger) {
            return new BigIntegerMixin(bigInteger, factory);
        }

        @Override
        public BigDecimalMixin visit(BigDecimalOptions bigDecimal) {
            return new BigDecimalMixin(bigDecimal, factory);
        }

        @Override
        public SkipMixin visit(SkipOptions skip) {
            return new SkipMixin(skip, factory);
        }

        @Override
        public TupleMixin visit(TupleOptions tuple) {
            return new TupleMixin(tuple, factory);
        }

        @Override
        public TypedObjectMixin visit(TypedObjectOptions typedObject) {
            return new TypedObjectMixin(typedObject, factory);
        }

        @Override
        public LocalDateMixin visit(LocalDateOptions localDate) {
            return new LocalDateMixin(localDate, factory);
        }

        @Override
        public ArrayMixin visit(ArrayOptions array) {
            return new ArrayMixin(array, factory);
        }

        @Override
        public AnyMixin visit(AnyOptions any) {
            return new AnyMixin(any, factory);
        }
    }
}
