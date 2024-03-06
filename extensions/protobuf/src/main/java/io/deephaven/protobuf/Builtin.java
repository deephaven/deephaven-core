//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.function.ToBooleanFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.protobuf.FieldOptions.BytesBehavior;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

class Builtin {

    // Due to https://github.com/confluentinc/schema-registry/issues/2708, the parsers need to be built in such a way
    // that they work with DynamicMessages.

    static List<MessageParser> parsers() {
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

    static <T extends Message> MessageParserSingle customParser(Class<T> clazz) {
        try {
            final Method method = clazz.getDeclaredMethod("getDescriptor");
            final Descriptor descriptor = (Descriptor) method.invoke(null);
            return new GenericSVMP<>(CustomType.of(clazz), descriptor);
        } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private enum TimestampParser implements MessageParserSingle {
        INSTANCE;

        private static final ToObjectFunction<Message, Instant> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, Timestamp>identity(Type.ofCustom(Timestamp.class))
                .mapToObj(TimestampParser::parseCanonical, Type.instantType());

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        private static Instant parseCanonical(Timestamp timestamp) {
            return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return Timestamp.getDescriptor();
        }

        @Override
        public ToObjectFunction<Message, Instant> messageParser(Descriptor descriptor,
                ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return TimestampFunction.of(descriptor);
        }
    }

    private enum DurationParser implements MessageParserSingle {
        INSTANCE;

        private static final ToObjectFunction<Message, Duration> CANONICAL_FUNCTION =
                ToObjectFunction.<Message, com.google.protobuf.Duration>identity(
                        Type.ofCustom(com.google.protobuf.Duration.class))
                        .mapToObj(DurationParser::parseCanonical, Type.ofCustom(Duration.class));

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        private static Duration parseCanonical(com.google.protobuf.Duration duration) {
            return Duration.ofSeconds(duration.getSeconds(), duration.getNanos());
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return com.google.protobuf.Duration.getDescriptor();
        }

        @Override
        public ToObjectFunction<Message, Duration> messageParser(Descriptor descriptor,
                ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return DurationFunction.of(descriptor);
        }
    }

    private enum BoolValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToBooleanFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, BoolValue>identity(Type.ofCustom(BoolValue.class))
                .mapToBoolean(BoolValue::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return BoolValue.getDescriptor();
        }

        @Override
        public ToBooleanFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new BoolFieldFunction(descriptor.findFieldByNumber(BoolValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum Int32ValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToIntFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, Int32Value>identity(Type.ofCustom(Int32Value.class))
                .mapToInt(Int32Value::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return Int32Value.getDescriptor();
        }

        @Override
        public ToIntFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new IntFieldFunction(descriptor.findFieldByNumber(Int32Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum UInt32ValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToIntFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, UInt32Value>identity(Type.ofCustom(UInt32Value.class))
                .mapToInt(UInt32Value::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return UInt32Value.getDescriptor();
        }

        @Override
        public ToIntFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new IntFieldFunction(descriptor.findFieldByNumber(UInt32Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum Int64ValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToLongFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, Int64Value>identity(Type.ofCustom(Int64Value.class))
                .mapToLong(Int64Value::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return Int64Value.getDescriptor();
        }

        @Override
        public ToLongFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new LongFieldFunction(descriptor.findFieldByNumber(Int64Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum UInt64ValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToLongFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, UInt64Value>identity(Type.ofCustom(UInt64Value.class))
                .mapToLong(UInt64Value::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return UInt64Value.getDescriptor();
        }

        @Override
        public ToLongFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new LongFieldFunction(descriptor.findFieldByNumber(UInt64Value.VALUE_FIELD_NUMBER));
        }
    }

    private enum FloatValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToFloatFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, FloatValue>identity(Type.ofCustom(FloatValue.class))
                .mapToFloat(FloatValue::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return FloatValue.getDescriptor();
        }

        @Override
        public ToFloatFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new FloatFieldFunction(descriptor.findFieldByNumber(FloatValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum DoubleValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToDoubleFunction<Message> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, DoubleValue>identity(Type.ofCustom(DoubleValue.class))
                .mapToDouble(DoubleValue::getValue);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return DoubleValue.getDescriptor();
        }

        @Override
        public ToDoubleFunction<Message> messageParser(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new DoubleFieldFunction(descriptor.findFieldByNumber(DoubleValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum StringValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToObjectFunction<Message, String> CANONICAL_FUNCTION = ToObjectFunction
                .<Message, StringValue>identity(Type.ofCustom(StringValue.class))
                .mapToObj(StringValue::getValue, Type.stringType());

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return StringValue.getDescriptor();
        }

        @Override
        public ToObjectFunction<Message, String> messageParser(Descriptor descriptor,
                ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            if (canonicalDescriptor() == descriptor) {
                return CANONICAL_FUNCTION;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            return new StringFieldFunction(descriptor.findFieldByNumber(StringValue.VALUE_FIELD_NUMBER));
        }
    }

    private enum BytesValueParser implements MessageParserSingle {
        INSTANCE;

        private static final ToObjectFunction<Message, ByteString> CANONICAL_FUNCTION_BYTESTRING = ToObjectFunction
                .<Message, BytesValue>identity(Type.ofCustom(BytesValue.class))
                .mapToObj(BytesValue::getValue, Type.ofCustom(ByteString.class));
        private static final ToObjectFunction<ByteString, byte[]> BYTESTRING_TO_BYTES =
                ToObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType());
        private static final ToObjectFunction<Message, byte[]> CANONICAL_FUNCTION_BYTEARRAY =
                CANONICAL_FUNCTION_BYTESTRING.mapToObj(BYTESTRING_TO_BYTES);

        public static MessageParserSingle of() {
            return INSTANCE;
        }

        @Override
        public Descriptor canonicalDescriptor() {
            return BytesValue.getDescriptor();
        }

        @Override
        public ToObjectFunction<Message, ?> messageParser(Descriptor descriptor,
                ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            final boolean asByteArray = options.fieldOptions().apply(fieldPath).bytes() == BytesBehavior.asByteArray();
            if (canonicalDescriptor() == descriptor) {
                return asByteArray
                        ? CANONICAL_FUNCTION_BYTEARRAY
                        : CANONICAL_FUNCTION_BYTESTRING;
            }
            checkCompatible(canonicalDescriptor(), descriptor);
            final ByteStringFieldFunction bsf =
                    new ByteStringFieldFunction(descriptor.findFieldByNumber(BytesValue.VALUE_FIELD_NUMBER));
            return asByteArray
                    ? bsf.mapToObj(BYTESTRING_TO_BYTES)
                    : bsf;
        }
    }

    private static class GenericSVMP<T extends Message> implements MessageParserSingle {
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
        public ToObjectFunction<Message, T> messageParser(Descriptor descriptor,
                ProtobufDescriptorParserOptions options,
                FieldPath fieldPath) {
            checkCompatible(canonicalDescriptor(), descriptor);
            return ToObjectFunction.identity(type);
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

    private static final class TimestampFunction implements ToObjectFunction<Message, Instant> {

        public static ToObjectFunction<Message, Instant> of(Descriptor descriptor) {
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

    private static final class DurationFunction implements ToObjectFunction<Message, Duration> {

        private static final GenericType<Duration> RETURN_TYPE = Type.ofCustom(Duration.class);

        public static ToObjectFunction<Message, Duration> of(Descriptor descriptor) {
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

    private static final class BoolFieldFunction implements ToBooleanFunction<Message> {
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

    private static final class IntFieldFunction implements ToIntFunction<Message> {

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

    private static final class LongFieldFunction implements ToLongFunction<Message> {

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

    private static final class FloatFieldFunction implements ToFloatFunction<Message> {
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

    private static final class DoubleFieldFunction implements ToDoubleFunction<Message> {
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

    private static final class StringFieldFunction implements ToObjectFunction<Message, String> {
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

    private static final class ByteStringFieldFunction implements ToObjectFunction<Message, ByteString> {
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
