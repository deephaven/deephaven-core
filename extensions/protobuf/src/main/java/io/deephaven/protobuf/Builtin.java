/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.FieldMask;
import com.google.protobuf.FloatValue;
import com.google.protobuf.Int32Value;
import com.google.protobuf.Int64Value;
import com.google.protobuf.Message;
import com.google.protobuf.StringValue;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UInt32Value;
import com.google.protobuf.UInt64Value;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.TypedFunction;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

class Builtin {
    static List<SingleValuedMessageParser> parsers() {
        // Update javadoc in io.deephaven.protobuf.SingleValuedMessageParser.builtin when editing
        return List.of(
                TimestampParser.of(),
                DurationParser.of(),
                BoolValueParser.of(),
                Int32ValueParser.of(),
                UInt32ValueParser.of(),
                Int64ValueParser.of(),
                UInt64ValueParser.of(),
                FloatValueParser.of(),
                DoubleValueParser.of(),
                StringValueParser.of(),
                BytesValueParser.of(),
                customParser(Any.class),
                customParser(FieldMask.class));
    }

    static <T extends Message> SingleValuedMessageParser customParser(Class<T> clazz) {
        try {
            final Method method = clazz.getDeclaredMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) method.invoke(null);
            return new GenericSVMP<>(CustomType.of(clazz), descriptor);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private enum TimestampParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return Timestamp.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return TimestampFunction.of(descriptor);
        }
    }

    private enum DurationParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return com.google.protobuf.Duration.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return DurationFunction.of(descriptor);
        }
    }

    private enum BoolValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return BoolValue.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new BoolFieldFunction(descriptor.findFieldByNumber(BoolValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum Int32ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return Int32Value.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new IntFieldFunction(descriptor.findFieldByNumber(Int32Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum UInt32ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return UInt32Value.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new IntFieldFunction(descriptor.findFieldByNumber(UInt32Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum Int64ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return Int64Value.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new LongFieldFunction(descriptor.findFieldByNumber(Int64Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum UInt64ValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return UInt64Value.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new LongFieldFunction(descriptor.findFieldByNumber(UInt64Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum FloatValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return FloatValue.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new FloatFieldFunction(descriptor.findFieldByNumber(FloatValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum DoubleValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return DoubleValue.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new DoubleFieldFunction(descriptor.findFieldByNumber(DoubleValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum StringValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return StringValue.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new StringFieldFunction(descriptor.findFieldByNumber(StringValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum BytesValueParser implements SingleValuedMessageParser {
        INSTANCE;

        public static SingleValuedMessageParser of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return BytesValue.getDescriptor();
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return new ByteStringFieldFunction(descriptor.findFieldByNumber(BytesValue.VALUE_FIELD_NUMBER))
                    .mapObj(ByteString::toByteArray, Type.byteType().arrayType());
        }
    }

    private static class GenericSVMP<T extends Message> implements SingleValuedMessageParser {
        private final GenericType<T> type;
        private final Descriptor descriptor;

        public GenericSVMP(GenericType<T> type, Descriptor descriptor) {
            this.type = Objects.requireNonNull(type);
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return descriptor;
        }

        @Override
        public TypedFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return ObjectFunction.identity(type);
        }
    }

    private static void checkCompatible(Descriptor expected, Descriptor actual) {
        final String expectedName = expected.getFullName();
        final String actualName = actual.getFullName();
        if (!expectedName.equals(actualName)) {
            throw new IllegalArgumentException(
                    String.format("Incompatible descriptors, expected=%s, actual=%s", expectedName, actualName));
        }
    }

    private static void checkCompatible(JavaType expected, FieldDescriptor actual) {
        if (!expected.equals(actual.getJavaType())) {
            throw new IllegalArgumentException(String.format("Incompatible field type, expected=%s, actual=%s (%s)",
                    expected, actual.getJavaType(), actual.getFullName()));
        }
    }

    private static final class TimestampFunction implements ObjectFunction<Message, Instant> {

        public static ObjectFunction<Message, Instant> of(Descriptor descriptor) {
            final FieldDescriptor secondsField = descriptor.findFieldByNumber(Timestamp.SECONDS_FIELD_NUMBER);
            final FieldDescriptor nanosField = descriptor.findFieldByNumber(Timestamp.NANOS_FIELD_NUMBER);
            return new TimestampFunction(secondsField, nanosField);
        }

        private final FieldDescriptor seconds;
        private final FieldDescriptor nanos;

        private TimestampFunction(FieldDescriptor seconds, FieldDescriptor nanos) {
            checkCompatible(JavaType.LONG, seconds);
            checkCompatible(JavaType.INT, nanos);
            this.seconds = Objects.requireNonNull(seconds);
            this.nanos = Objects.requireNonNull(nanos);
        }

        @Override
        public GenericType<Instant> returnType() {
            return Type.instantType();
        }

        @Override
        public Instant apply(Message value) {
            return Instant.ofEpochSecond((long) value.getField(seconds), (int) value.getField(nanos));
        }
    }

    private static final class DurationFunction implements ObjectFunction<Message, Duration> {

        private static final GenericType<Duration> RETURN_TYPE = Type.ofCustom(Duration.class);

        public static ObjectFunction<Message, Duration> of(Descriptor descriptor) {
            final FieldDescriptor secondsField =
                    descriptor.findFieldByNumber(com.google.protobuf.Duration.SECONDS_FIELD_NUMBER);
            final FieldDescriptor nanosField =
                    descriptor.findFieldByNumber(com.google.protobuf.Duration.NANOS_FIELD_NUMBER);
            return new DurationFunction(secondsField, nanosField);
        }

        private final FieldDescriptor seconds;
        private final FieldDescriptor nanos;

        private DurationFunction(FieldDescriptor seconds, FieldDescriptor nanos) {
            checkCompatible(JavaType.LONG, seconds);
            checkCompatible(JavaType.INT, nanos);
            this.seconds = Objects.requireNonNull(seconds);
            this.nanos = Objects.requireNonNull(nanos);
        }

        @Override
        public GenericType<Duration> returnType() {
            return RETURN_TYPE;
        }

        @Override
        public Duration apply(Message value) {
            return Duration.ofSeconds((long) value.getField(seconds), (int) value.getField(nanos));
        }
    }

    private static final class BoolFieldFunction implements BooleanFunction<Message> {
        private final FieldDescriptor valueField;

        public BoolFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.BOOLEAN, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public boolean test(Message value) {
            return (boolean) value.getField(valueField);
        }
    }

    private static final class IntFieldFunction implements IntFunction<Message> {

        private final FieldDescriptor valueField;

        public IntFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.INT, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public int applyAsInt(Message value) {
            return (int) value.getField(valueField);
        }
    }

    private static final class LongFieldFunction implements LongFunction<Message> {

        private final FieldDescriptor valueField;

        public LongFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.LONG, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public long applyAsLong(Message value) {
            return (long) value.getField(valueField);
        }
    }

    private static final class FloatFieldFunction implements FloatFunction<Message> {
        private final FieldDescriptor valueField;

        public FloatFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.FLOAT, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public float applyAsFloat(Message value) {
            return (float) value.getField(valueField);
        }
    }

    private static final class DoubleFieldFunction implements DoubleFunction<Message> {
        private final FieldDescriptor valueField;

        public DoubleFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.DOUBLE, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public double applyAsDouble(Message value) {
            return (double) value.getField(valueField);
        }
    }

    private static final class StringFieldFunction implements ObjectFunction<Message, String> {
        private final FieldDescriptor valueField;

        public StringFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.STRING, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public GenericType<String> returnType() {
            return Type.stringType();
        }

        @Override
        public String apply(Message value) {
            return (String) value.getField(valueField);
        }
    }

    private static final class ByteStringFieldFunction implements ObjectFunction<Message, ByteString> {
        private static final CustomType<ByteString> RETURN_TYPE = Type.ofCustom(ByteString.class);

        private final FieldDescriptor valueField;

        public ByteStringFieldFunction(FieldDescriptor valueField) {
            checkCompatible(JavaType.BYTE_STRING, valueField);
            this.valueField = Objects.requireNonNull(valueField);
        }

        @Override
        public GenericType<ByteString> returnType() {
            return RETURN_TYPE;
        }

        @Override
        public ByteString apply(Message value) {
            return (ByteString) value.getField(valueField);
        }
    }
}
