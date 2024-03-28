//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.engine.table.TableDefinition;
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
import io.deephaven.json.JsonStreamPublisher;
import io.deephaven.json.JsonStreamPublisherOptions;
import io.deephaven.json.LocalDateOptions;
import io.deephaven.json.LongOptions;
import io.deephaven.json.ObjectFieldOptions;
import io.deephaven.json.ObjectKvOptions;
import io.deephaven.json.ObjectOptions;
import io.deephaven.json.ShortOptions;
import io.deephaven.json.SkipOptions;
import io.deephaven.json.Source;
import io.deephaven.json.StringOptions;
import io.deephaven.json.TupleOptions;
import io.deephaven.json.TypedObjectOptions;
import io.deephaven.json.ValueOptions;
import io.deephaven.json.ValueOptions.Visitor;
import io.deephaven.json.jackson.NavContext.JsonProcess;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class JacksonStreamPublisher implements JsonStreamPublisher {
    public static JacksonStreamPublisher of(JsonStreamPublisherOptions options, JsonFactory factory) {
        return new JacksonStreamPublisher(options, factory);
    }

    private final JsonStreamPublisherOptions publisherOptions;
    private final JsonFactory factory;
    private final boolean collapseArrayBoundary = true;
    private StreamConsumer consumer;
    private volatile boolean shutdown;

    private JacksonStreamPublisher(
            JsonStreamPublisherOptions publisherOptions,
            JsonFactory factory) {
        this.publisherOptions = Objects.requireNonNull(publisherOptions);
        this.factory = Objects.requireNonNull(factory);
    }

    @Override
    public TableDefinition tableDefinition(Function<List<String>, String> namingFunction) {
        DrivenJsonProcess current = process();
        while (!(current instanceof LeafBase)) {
            current = ((DrivenJsonProcessSingleDelegateBase<?>) current).delegate;
        }
        final Mixin<?> mixin = ((LeafBase<?>) current).mixin;
        return TableDefinition.from(mixin.names(namingFunction), types(mixin).collect(Collectors.toList()));
    }

    private static Stream<? extends Type<?>> types(Mixin<?> mixin) {
        return mixin.outputTypes();
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public void flush() {
        // unlike async streaming cases, the runners here are going as fast as possible, and hand off their buffers
        // asap.
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
    }

    @Override
    public void execute(Executor executor, Queue<Source> sources) {
        executor.execute(new Driver(sources, process()));
    }

    private DrivenJsonProcess process() {
        return new VisitorImpl().of(publisherOptions.options());
    }

    interface DrivenJsonProcess extends JsonProcess {

        @Override
        void process(JsonParser parser) throws IOException;

        // needs to know when it's done
        void done();
    }

    interface LeafJsonProcess extends DrivenJsonProcess {

        void prepare(List<WritableChunk<?>> out);

        void incAndMaybeFlush();
    }

    static abstract class DrivenJsonProcessSingleDelegateBase<T extends ValueOptions> implements DrivenJsonProcess {
        final T options;
        final DrivenJsonProcess delegate;

        DrivenJsonProcessSingleDelegateBase(T options, DrivenJsonProcess delegate) {
            this.options = Objects.requireNonNull(options);
            this.delegate = Objects.requireNonNull(delegate);
        }

        @Override
        public final void done() {
            delegate.done();
        }
    }

    private final class Driver implements Runnable {
        private final Queue<Source> sources;
        private final DrivenJsonProcess process;

        public Driver(Queue<Source> sources, DrivenJsonProcess process) {
            this.sources = Objects.requireNonNull(sources);
            this.process = Objects.requireNonNull(process);
        }

        @Override
        public void run() {
            try {
                runImpl();
            } catch (Throwable t) {
                consumer.acceptFailure(t);
            }
        }

        private void runImpl() throws IOException {
            Source source;
            while ((source = sources.poll()) != null) {
                try (final Stream<Source> stream = Source.stream(source)) {
                    final Iterator<Source> it = stream.iterator();
                    while (it.hasNext()) {
                        try (final JsonParser parser = JacksonSource.of(factory, it.next())) {
                            parser.nextToken();
                            do {
                                // todo: inner process should check for shutdown too
                                process.process(parser);
                                parser.nextToken();
                            } while (publisherOptions.multiValueSupport() && !parser.hasToken(null) && !shutdown);
                            // todo: throw exception if not multi value and not null?
                        }
                    }
                }
            }
            process.done();
        }
    }

    private static class ArrayProcess extends DrivenJsonProcessSingleDelegateBase<ArrayOptions> {

        public ArrayProcess(ArrayOptions options, DrivenJsonProcess elementDelegate) {
            super(options, elementDelegate);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            // does callback here need to increment parser?
            ValueProcessor.processArray(parser, delegate, null);
        }
    }

    private static class ObjectFieldProcess extends DrivenJsonProcessSingleDelegateBase<ObjectOptions> {
        private final ObjectFieldOptions field;

        public ObjectFieldProcess(ObjectOptions options, ObjectFieldOptions field, DrivenJsonProcess fieldDelegate) {
            super(options, fieldDelegate);
            this.field = Objects.requireNonNull(field);
            if (!options.fields().contains(field)) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            NavContext.processObjectField(parser, options, field, delegate);
        }
    }

    private static class TupleIndexProcess extends DrivenJsonProcessSingleDelegateBase<TupleOptions> {
        private final int index;

        public TupleIndexProcess(TupleOptions tuple, int index, DrivenJsonProcess elementDelegate) {
            super(tuple, elementDelegate);
            this.index = index;
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            // todo: options for error handling
            NavContext.processTupleIndex(parser, index, delegate);
        }
    }

    private abstract class LeafBase<T extends Mixin<?>> implements LeafJsonProcess {
        final T mixin;
        private final List<ChunkType> chunkTypes;
        @SuppressWarnings("rawtypes")
        private final WritableChunk[] outArray;
        private List<WritableChunk<?>> out;
        private int count;

        LeafBase(T mixin, List<ChunkType> chunkTypes) {
            this.mixin = Objects.requireNonNull(mixin);
            this.chunkTypes = Objects.requireNonNull(chunkTypes);
            this.outArray = new WritableChunk[chunkTypes.size()];
            prepareImpl();
        }

        @Override
        public final void incAndMaybeFlush() {
            if (shutdown) {
                throw new RuntimeException("shutdown");
            }
            count++;
            if (count == publisherOptions.chunkSize()) {
                flushImpl();
            }
        }

        @Override
        public final void done() {
            if (count != 0) {
                flushImpl();
            }
        }

        private void flushImpl() {
            // noinspection unchecked
            consumer.accept(out.toArray(outArray));
            prepareImpl();
        }

        private void prepareImpl() {
            out = newProcessorChunks(publisherOptions.chunkSize(), chunkTypes);
            prepare(out);
            count = 0;
        }
    }

    private final class SingleValueLeaf extends LeafBase<Mixin<?>> {
        private ValueProcessor processor;

        public SingleValueLeaf(Mixin<?> mixin) {
            super(mixin, mixin.outputTypes().map(ObjectProcessor::chunkType).collect(Collectors.toList()));
        }

        @Override
        public void prepare(List<WritableChunk<?>> out) {
            processor = mixin.processor("todo", out);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            ValueProcessor.process(parser, processor);
            incAndMaybeFlush();
        }
    }

    private final class ArrayLeaf extends LeafBase<ArrayMixin> {
        private ValueProcessor elementDelegate;

        public ArrayLeaf(ArrayMixin mixin) {
            super(mixin, mixin.elementOutputTypes().map(ObjectProcessor::chunkType).collect(Collectors.toList()));
        }

        @Override
        public void prepare(List<WritableChunk<?>> out) {
            elementDelegate = mixin.elementAsValueProcessor(out);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            ValueProcessor.processArray(parser, elementDelegate::processCurrentValue, this::incAndMaybeFlush);
        }
    }

    private final class KeyValueLeaf extends LeafBase<ObjectKvMixin> {
        private ValueProcessor key;
        private ValueProcessor value;

        public KeyValueLeaf(ObjectKvMixin mixin) {
            super(mixin, mixin.keyValueOutputTypes().map(ObjectProcessor::chunkType).collect(Collectors.toList()));
        }

        @Override
        public void prepare(List<WritableChunk<?>> out) {
            key = mixin.keyAsValueProcessor(out);
            value = mixin.valueAsValueProcessor(out);
        }

        @Override
        public void process(JsonParser parser) throws IOException {
            ValueProcessor.processKeyValues(parser, key::processCurrentValue, value::processCurrentValue,
                    this::incAndMaybeFlush);
        }
    }

    private static List<WritableChunk<?>> newProcessorChunks(int chunkSize, List<ChunkType> types) {
        return types
                .stream()
                .map(chunkType -> chunkType.makeWritableChunk(chunkSize))
                .peek(wc -> wc.setSize(0))
                .collect(Collectors.toList());
    }

    class VisitorImpl implements Visitor<DrivenJsonProcess> {

        DrivenJsonProcess of(ValueOptions options) {
            return Objects.requireNonNull(options.walk(this));
        }

        DrivenJsonProcess of(ArrayOptions array) {
            return collapseArrayBoundary
                    ? new ArrayProcess(array, of(array.element()))
                    : new ArrayLeaf(new ArrayMixin(array, factory));
        }

        DrivenJsonProcess of(ObjectKvOptions kvOptions) {
            // can't boundary collapse past KV right now; logic is much more complicated due to the bifurcation of
            // key AND value. We _could_ try to do it when key or value doesn't produce any output.
            return new KeyValueLeaf(new ObjectKvMixin(kvOptions, factory));
        }

        DrivenJsonProcess of(ObjectOptions object) {
            if (object.fields().size() == 1) {
                final ObjectFieldOptions field = object.fields().iterator().next();
                return new ObjectFieldProcess(object, field, of(field.options()));
            }
            return single(object);
        }

        DrivenJsonProcess of(TupleOptions tuple) {
            // todo: see if all are skips except one
            if (tuple.values().size() == 1) {
                final ValueOptions element = tuple.values().get(0);
                return new TupleIndexProcess(tuple, 0, of(element));
            }
            return single(tuple);
        }

        SingleValueLeaf single(ValueOptions options) {
            return new SingleValueLeaf(Mixin.of(options, factory));
        }

        @Override
        public DrivenJsonProcess visit(ArrayOptions array) {
            return of(array);
        }

        @Override
        public DrivenJsonProcess visit(ObjectKvOptions objectKv) {
            return of(objectKv);
        }

        @Override
        public DrivenJsonProcess visit(ObjectOptions object) {
            return of(object);
        }

        @Override
        public DrivenJsonProcess visit(TupleOptions tuple) {
            return of(tuple);
        }

        @Override
        public DrivenJsonProcess visit(StringOptions _string) {
            return single(_string);
        }

        @Override
        public DrivenJsonProcess visit(BoolOptions _bool) {
            return single(_bool);
        }

        @Override
        public DrivenJsonProcess visit(CharOptions _char) {
            return single(_char);
        }

        @Override
        public DrivenJsonProcess visit(ByteOptions _byte) {
            return single(_byte);
        }

        @Override
        public DrivenJsonProcess visit(ShortOptions _short) {
            return single(_short);
        }

        @Override
        public DrivenJsonProcess visit(IntOptions _int) {
            return single(_int);
        }

        @Override
        public DrivenJsonProcess visit(LongOptions _long) {
            return single(_long);
        }

        @Override
        public DrivenJsonProcess visit(FloatOptions _float) {
            return single(_float);
        }

        @Override
        public DrivenJsonProcess visit(DoubleOptions _double) {
            return single(_double);
        }

        @Override
        public DrivenJsonProcess visit(InstantOptions instant) {
            return single(instant);
        }

        @Override
        public DrivenJsonProcess visit(InstantNumberOptions instantNumber) {
            return single(instantNumber);
        }

        @Override
        public DrivenJsonProcess visit(BigIntegerOptions bigInteger) {
            return single(bigInteger);
        }

        @Override
        public DrivenJsonProcess visit(BigDecimalOptions bigDecimal) {
            return single(bigDecimal);
        }

        @Override
        public DrivenJsonProcess visit(SkipOptions skip) {
            return single(skip);
        }

        @Override
        public DrivenJsonProcess visit(TypedObjectOptions typedObject) {
            return single(typedObject);
        }

        @Override
        public DrivenJsonProcess visit(LocalDateOptions localDate) {
            return single(localDate);
        }

        @Override
        public DrivenJsonProcess visit(AnyOptions any) {
            return single(any);
        }
    }
}
