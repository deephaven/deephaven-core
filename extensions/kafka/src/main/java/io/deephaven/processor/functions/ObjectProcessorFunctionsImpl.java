/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.processor.functions;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToPrimitiveFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.kafka.ingest.ChunkUtils;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.type.TypeUtils;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

final class ObjectProcessorFunctionsImpl<T> implements ObjectProcessor<T> {

    interface Appender<T> {

        void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest);
    }

    static <T> ObjectProcessorFunctionsImpl<T> create(List<TypedFunction<? super T>> functions) {
        final List<Type<?>> logicalTypes = functions.stream()
                .map(TypedFunction::returnType)
                .collect(Collectors.toList());
        final List<Appender<? super T>> appenders = functions.stream()
                .map(AppenderVisitor::of)
                .collect(Collectors.toList());
        return new ObjectProcessorFunctionsImpl<>(logicalTypes, appenders);
    }

    private final List<Type<?>> logicalTypes;
    private final List<Appender<? super T>> appenders;

    private ObjectProcessorFunctionsImpl(List<Type<?>> logicalTypes, List<Appender<? super T>> appenders) {
        this.logicalTypes = List.copyOf(Objects.requireNonNull(logicalTypes));
        this.appenders = Objects.requireNonNull(appenders);
    }

    @Override
    public List<Type<?>> outputTypes() {
        return logicalTypes;
    }

    @Override
    public void processAll(ObjectChunk<? extends T, ?> in, List<WritableChunk<?>> out) {
        checkChunks(out);
        final int L = appenders.size();
        for (int i = 0; i < L; ++i) {
            appenders.get(i).append(in, out.get(i));
        }
    }

    private void checkChunks(List<WritableChunk<?>> out) {
        final int numColumns = appenders.size();
        if (numColumns != out.size()) {
            throw new IllegalArgumentException(String.format(
                    "Expected appenders.size() == out.size(). appenders.size()=%d, out.size()=%d",
                    numColumns, out.size()));
        }
        // we'll catch mismatched chunk types later when we try to cast them
    }

    private static class AppenderVisitor<T> implements
            TypedFunction.Visitor<T, Appender<T>>,
            ToPrimitiveFunction.Visitor<T, Appender<T>> {

        static <T> Appender<T> of(TypedFunction<T> f) {
            return f.walk(new AppenderVisitor<>());
        }

        @Override
        public Appender<T> visit(ToPrimitiveFunction<T> f) {
            return f.walk((ToPrimitiveFunction.Visitor<T, Appender<T>>) this);
        }

        @Override
        public ByteAppender<T> visit(ToBooleanFunction<T> f) {
            return ByteAppender.from(f);
        }

        @Override
        public CharAppender<T> visit(ToCharFunction<T> f) {
            return new CharAppender<>(f);
        }

        @Override
        public ByteAppender<T> visit(ToByteFunction<T> f) {
            return new ByteAppender<>(f);
        }

        @Override
        public ShortAppender<T> visit(ToShortFunction<T> f) {
            return new ShortAppender<>(f);
        }

        @Override
        public IntAppender<T> visit(ToIntFunction<T> f) {
            return new IntAppender<>(f);
        }

        @Override
        public LongAppender<T> visit(ToLongFunction<T> f) {
            return new LongAppender<>(f);
        }

        @Override
        public FloatAppender<T> visit(ToFloatFunction<T> f) {
            return new FloatAppender<>(f);
        }

        @Override
        public DoubleAppender<T> visit(ToDoubleFunction<T> f) {
            return new DoubleAppender<>(f);
        }

        @Override
        public Appender<T> visit(ToObjectFunction<T, ?> f) {
            return f.returnType().walk(new ObjectFunctionAppenderVisitor<>(this, f));
        }
    }

    private static class ObjectFunctionAppenderVisitor<T>
            implements GenericType.Visitor<Appender<T>>, BoxedType.Visitor<Appender<T>> {

        private final AppenderVisitor<T> v;
        private final ToObjectFunction<T, ?> f;

        private ObjectFunctionAppenderVisitor(AppenderVisitor<T> v, ToObjectFunction<T, ?> f) {
            this.v = Objects.requireNonNull(v);
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public Appender<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<Appender<T>>) this);
        }

        @Override
        public ByteAppender<T> visit(BoxedBooleanType booleanType) {
            return ByteAppender.from(f.cast(booleanType));
        }

        @Override
        public ByteAppender<T> visit(BoxedByteType byteType) {
            return v.visit(f.cast(byteType).mapToByte(TypeUtils::unbox));
        }

        @Override
        public CharAppender<T> visit(BoxedCharType charType) {
            return v.visit(f.cast(charType).mapToChar(TypeUtils::unbox));
        }

        @Override
        public ShortAppender<T> visit(BoxedShortType shortType) {
            return v.visit(f.cast(shortType).mapToShort(TypeUtils::unbox));
        }

        @Override
        public IntAppender<T> visit(BoxedIntType intType) {
            return v.visit(f.cast(intType).mapToInt(TypeUtils::unbox));
        }

        @Override
        public LongAppender<T> visit(BoxedLongType longType) {
            return v.visit(f.cast(longType).mapToLong(TypeUtils::unbox));
        }

        @Override
        public FloatAppender<T> visit(BoxedFloatType floatType) {
            return v.visit(f.cast(floatType).mapToFloat(TypeUtils::unbox));
        }

        @Override
        public DoubleAppender<T> visit(BoxedDoubleType doubleType) {
            return v.visit(f.cast(doubleType).mapToDouble(TypeUtils::unbox));
        }

        @Override
        public ObjectAppender<T> visit(StringType stringType) {
            return new ObjectAppender<>(f);
        }

        @Override
        public LongAppender<T> visit(InstantType instantType) {
            // to long function
            return v.visit(f.cast(instantType).mapToLong(DateTimeUtils::epochNanos));
        }

        @Override
        public ObjectAppender<T> visit(ArrayType<?, ?> arrayType) {
            return new ObjectAppender<>(f);
        }

        @Override
        public ObjectAppender<T> visit(CustomType<?> customType) {
            return new ObjectAppender<>(f);
        }
    }

    private static class ObjectAppender<T> implements Appender<T> {
        private final ToObjectFunction<? super T, ?> f;

        ObjectAppender(ToObjectFunction<? super T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableObjectChunk());
        }
    }

    private static class CharAppender<T> implements Appender<T> {
        private final ToCharFunction<? super T> f;

        CharAppender(ToCharFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableCharChunk());
        }
    }

    private static class ByteAppender<T> implements Appender<T> {

        static <T> ByteAppender<T> from(ToBooleanFunction<? super T> f) {
            return new ByteAppender<>(x -> BooleanUtils.booleanAsByte(f.test(x)));
        }

        static <T> ByteAppender<T> from(ToObjectFunction<? super T, ? extends Boolean> f) {
            return new ByteAppender<>(f.mapToByte(BooleanUtils::booleanAsByte));
        }

        private final ToByteFunction<? super T> f;

        ByteAppender(ToByteFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableByteChunk());
        }
    }

    private static class ShortAppender<T> implements Appender<T> {
        private final ToShortFunction<? super T> f;

        ShortAppender(ToShortFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableShortChunk());
        }
    }

    private static class IntAppender<T> implements Appender<T> {
        private final java.util.function.ToIntFunction<? super T> f;

        IntAppender(java.util.function.ToIntFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableIntChunk());
        }
    }

    private static class LongAppender<T> implements Appender<T> {
        private final java.util.function.ToLongFunction<? super T> f;

        LongAppender(java.util.function.ToLongFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableLongChunk());
        }
    }

    private static class FloatAppender<T> implements Appender<T> {
        private final ToFloatFunction<? super T> f;

        FloatAppender(ToFloatFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableFloatChunk());
        }
    }

    private static class DoubleAppender<T> implements Appender<T> {
        private final java.util.function.ToDoubleFunction<? super T> f;

        DoubleAppender(java.util.function.ToDoubleFunction<? super T> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public void append(ObjectChunk<? extends T, ?> src, WritableChunk<?> dest) {
            ObjectProcessorFunctionsImpl.append(src, f, dest.asWritableDoubleChunk());
        }
    }

    // Ideally, these would be built into WritableChunk impls

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            ToByteFunction<? super T> byteFunction,
            WritableByteChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(byteFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            ToCharFunction<? super T> charFunction,
            WritableCharChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(charFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            ToShortFunction<? super T> shortFunction,
            WritableShortChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(shortFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            java.util.function.ToIntFunction<? super T> intFunction,
            WritableIntChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(intFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            java.util.function.ToLongFunction<? super T> longFunction,
            WritableLongChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(longFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            ToFloatFunction<? super T> floatFunction,
            WritableFloatChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(floatFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T> void append(
            ObjectChunk<? extends T, ?> src,
            java.util.function.ToDoubleFunction<? super T> doubleFunction,
            WritableDoubleChunk<?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(doubleFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }

    private static <T, R> void append(
            ObjectChunk<? extends T, ?> src,
            Function<? super T, ? extends R> objFunction,
            WritableObjectChunk<R, ?> dest) {
        final int destSize = dest.size();
        ChunkUtils.applyInto(objFunction, src, 0, dest, destSize, src.size());
        dest.setSize(destSize + src.size());
    }
}
