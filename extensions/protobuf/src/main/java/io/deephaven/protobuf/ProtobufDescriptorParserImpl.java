//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import io.deephaven.protobuf.FieldOptions.BytesBehavior;
import io.deephaven.protobuf.FieldOptions.MapBehavior;
import io.deephaven.protobuf.FieldOptions.WellKnownBehavior;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.function.ToBooleanFunction;
import io.deephaven.function.ToByteFunction;
import io.deephaven.function.ToCharFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToPrimitiveFunction;
import io.deephaven.function.ToShortFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.function.TypedFunction.Visitor;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class ProtobufDescriptorParserImpl {

    private static final ToObjectFunction<Object, String> STRING_OBJ = ToObjectFunction.identity(Type.stringType());
    private static final ToObjectFunction<Object, Integer> BOXED_INT_OBJ = ToObjectFunction.identity(BoxedIntType.of());
    private static final ToObjectFunction<Object, Long> BOXED_LONG_OBJ = ToObjectFunction.identity(BoxedLongType.of());
    private static final ToObjectFunction<Object, Float> BOXED_FLOAT_OBJ =
            ToObjectFunction.identity(BoxedFloatType.of());
    private static final ToObjectFunction<Object, Double> BOXED_DOUBLE_OBJ =
            ToObjectFunction.identity(BoxedDoubleType.of());
    private static final ToObjectFunction<Object, Boolean> BOXED_BOOLEAN_OBJ =
            ToObjectFunction.identity(BoxedBooleanType.of());
    private static final ToObjectFunction<Object, Message> MESSAGE_OBJ =
            ToObjectFunction.identity(Type.ofCustom(Message.class));
    private static final ToObjectFunction<Object, ByteString> BYTE_STRING_OBJ =
            ToObjectFunction.identity(Type.ofCustom(ByteString.class));
    private static final ToObjectFunction<Object, EnumValueDescriptor> ENUM_VALUE_DESCRIPTOR_OBJ =
            ToObjectFunction.identity(Type.ofCustom(EnumValueDescriptor.class));
    private static final ToObjectFunction<ByteString, byte[]> BYTE_STRING_FUNCTION =
            BypassOnNull.of(ToObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType()));

    private final ProtobufDescriptorParserOptions options;
    private final Map<String, MessageParser> byFullName;

    public ProtobufDescriptorParserImpl(ProtobufDescriptorParserOptions options) {
        this.options = Objects.requireNonNull(options);
        this.byFullName = options.parsers().stream()
                .collect(Collectors.toMap(x -> x.canonicalDescriptor().getFullName(), Function.identity()));
    }

    public ProtobufFunctions translate(Descriptor descriptor) {
        return new DescriptorContext(FieldPath.empty(), descriptor).functions();
    }

    private class DescriptorContext {
        private final FieldPath fieldPath;
        private final Descriptor descriptor;

        public DescriptorContext(FieldPath fieldPath, Descriptor descriptor) {
            this.fieldPath = Objects.requireNonNull(fieldPath);
            this.descriptor = Objects.requireNonNull(descriptor);
        }

        private ProtobufFunctions functions() {
            final ProtobufFunctions wellKnown = wellKnown().orElse(null);
            if (wellKnown != null) {
                return wellKnown;
            }
            final Builder builder = ProtobufFunctions.builder();
            for (FieldContext fc : fcs()) {
                builder.addAllFunctions(fc.functions().functions());
            }
            return builder.build();
        }

        private Optional<ProtobufFunctions> wellKnown() {
            final MessageParser mp = byFullName.get(descriptor.getFullName());
            if (mp == null) {
                return Optional.empty();
            }
            if (options.fieldOptions().apply(fieldPath).wellKnown() == WellKnownBehavior.asWellKnown()) {
                return Optional.of(mp.messageParsers(descriptor, options, fieldPath));
            }
            return Optional.empty();
        }

        private List<FieldContext> fcs() {
            return descriptor.getFields().stream().map(this::fc).collect(Collectors.toList());
        }

        private FieldContext fc(FieldDescriptor fd) {
            return new FieldContext(this, fd);
        }
    }

    private class FieldContext {
        private final DescriptorContext parent;
        private final FieldDescriptor fd;
        private final FieldPath fieldPath;

        public FieldContext(DescriptorContext parent, FieldDescriptor fd) {
            this.parent = Objects.requireNonNull(parent);
            this.fd = Objects.requireNonNull(fd);
            this.fieldPath = append(parent.fieldPath, fd);
        }

        private ProtobufFunctions functions() {
            return functions(false);
        }

        private ProtobufFunctions functions(boolean forceInclude) {
            final FieldOptions fo = options.fieldOptions().apply(fieldPath);
            if (!forceInclude && !fo.include()) {
                return ProtobufFunctions.empty();
            }
            if (fd.isMapField() && fo.map() == MapBehavior.asMap()) {
                return new MapFieldObject().functions();
            }
            if (fd.isRepeated()) {
                return new RepeatedFieldObject().functions();
            }
            return new FieldObject().functions();
        }

        private ProtobufFunctions namedField(TypedFunction<Message> tf) {
            return ProtobufFunctions.of(ProtobufFunction.of(FieldPath.of(fd), tf));
        }

        private DescriptorContext toMessageContext() {
            if (fd.getJavaType() != JavaType.MESSAGE) {
                throw new IllegalStateException();
            }
            return new DescriptorContext(fieldPath, fd.getMessageType());
        }

        private class FieldObject implements ToObjectFunction<Message, Object> {

            @Override
            public GenericType<Object> returnType() {
                return Type.ofCustom(Object.class);
            }

            @Override
            public Object apply(Message message) {
                // Note: in protobuf an actualized Message is never null - getField will always return non-null (and
                // thus, sub Messages will never be natively null).
                //
                // In the case of our translation layer though, the presence of a null Message means that this field, or
                // some parent of this field, was "not present".
                if (message == null) {
                    return null;
                }
                // hasField only semantically meaningful when hasPresence == true
                if (fd.hasPresence() && !message.hasField(fd)) {
                    return null;
                }
                return message.getField(fd);
            }

            private ProtobufFunctions functions() {
                // Note: we might be tempted at this layer to treat null boxed primitives as DH null primitives, but
                // 1) this parsing layer doesn't / shouldn't need to know about DH nulls
                // 2) protobuf already has the null object, so it doesn't harm us to propagate it to the calling layer,
                // and for the calling layer to unbox if desired.
                switch (fd.getJavaType()) {
                    case INT:
                        return fd.hasPresence()
                                ? namedField(mapToObj(BOXED_INT_OBJ))
                                : namedField(mapToInt(ToIntFunction.cast()));
                    case LONG:
                        return fd.hasPresence()
                                ? namedField(mapToObj(BOXED_LONG_OBJ))
                                : namedField(mapToLong(ToLongFunction.cast()));
                    case FLOAT:
                        return fd.hasPresence()
                                ? namedField(mapToObj(BOXED_FLOAT_OBJ))
                                : namedField(mapToFloat(ToFloatFunction.cast()));
                    case DOUBLE:
                        return fd.hasPresence()
                                ? namedField(mapToObj(BOXED_DOUBLE_OBJ))
                                : namedField(mapToDouble(ToDoubleFunction.cast()));
                    case BOOLEAN:
                        return fd.hasPresence()
                                ? namedField(mapToObj(BOXED_BOOLEAN_OBJ))
                                : namedField(mapToBoolean(ToBooleanFunction.cast()));
                    case STRING:
                        return namedField(mapToObj(STRING_OBJ));
                    case BYTE_STRING:
                        return options.fieldOptions().apply(fieldPath).bytes() == BytesBehavior.asByteArray()
                                ? namedField(mapToObj(BYTE_STRING_OBJ).mapToObj(BYTE_STRING_FUNCTION))
                                : namedField(mapToObj(BYTE_STRING_OBJ));
                    case ENUM:
                        return namedField(mapToObj(ENUM_VALUE_DESCRIPTOR_OBJ));
                    case MESSAGE: {
                        final ToObjectFunction<Message, Message> fieldAsMessage = mapToObj(MESSAGE_OBJ);
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions subF = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();
                        final boolean parentFieldIsRepeated = !parent.fieldPath.path().isEmpty()
                                && parent.fieldPath.path().get(parent.fieldPath.path().size() - 1).isRepeated();
                        for (ProtobufFunction e : subF.functions()) {
                            // The majority of the time, we need to BypassOnNull b/c the Message may be null. In the
                            // case where the message is part of a repeated field though, the Message is never null
                            // (it's always implicitly present based on the repeated count).
                            final TypedFunction<Message> value = parentFieldIsRepeated
                                    ? e.function()
                                    : BypassOnNull.of(e.function());
                            builder.addFunctions(
                                    ProtobufFunction.of(prepend(e.path(), fd), fieldAsMessage.map(value)));
                        }
                        return builder.build();
                    }
                    default:
                        throw new IllegalStateException();
                }
            }
        }

        private class MapFieldObject {

            private ProtobufFunctions functions() {
                // https://protobuf.dev/programming-guides/proto3/#maps
                // For maps fields:
                // map<KeyType, ValueType> map_field = 1;
                // The parsed descriptor looks like:
                // @formatter:off
                // message MapFieldEntry {
                //     option map_entry = true;
                //     optional KeyType key = 1;
                //     optional ValueType value = 2;
                // }
                // repeated MapFieldEntry map_field = 1;
                // @formatter:on

                if (fd.getMessageType().getFields().size() != 2) {
                    throw new IllegalStateException("Expected map to have exactly 2 field descriptors");
                }
                final FieldDescriptor keyFd = fd.getMessageType().findFieldByNumber(1);
                if (keyFd == null) {
                    throw new IllegalStateException("Expected map to have field descriptor number 1 (key)");
                }
                final FieldDescriptor valueFd = fd.getMessageType().findFieldByNumber(2);
                if (valueFd == null) {
                    throw new IllegalStateException("Expected map to have field descriptor number 2 (value)");
                }
                final DescriptorContext dc = new DescriptorContext(append(parent.fieldPath, fd), fd.getMessageType());

                // Note: maps are a "special" case, where even though we don't include the key / value FDs as a return
                // io.deephaven.protobuf.ProtobufFunction#path, it's important that we force their inclusion if we've
                // gotten this far.
                //
                // For example, if we have the schema:
                // @formatter:off
                // message MyMessage {
                //     map<int32, int32> my_map = 1;
                // }
                // @formatter:on
                //
                // The user should be able to use:
                // @formatter:off
                // ProtobufDescriptorParserOptions.builder()
                //     .fieldOptions(FieldOptions.includeIf(fp -> fp.namePath().equals(List.of("my_map"))))
                //     .build();
                // @formatter:on
                //
                // This involves parsing ["my_map", "key"] and ["my_map", "value"].
                //
                // See io.deephaven.protobuf.ProtobufDescriptorParserTest#intIntMapRestrictiveInclude
                final ProtobufFunctions keyFunctions = new FieldContext(dc, keyFd).functions(true);
                if (keyFunctions.functions().size() != 1) {
                    throw new IllegalStateException("Protobuf map keys must be a single type");
                }

                final ProtobufFunctions valueFunctions = new FieldContext(dc, valueFd).functions(true);
                if (valueFunctions.functions().size() != 1) {
                    // We've parsed the value type as an entity that has multiple values (as opposed to a single value
                    // we can put into a map). We may wish to have more configuration options for these situations in
                    // the future (ie, throw an exception or something else). For now, we're going to treat this case as
                    // "simple" repeated type.
                    return delegate();
                }

                final TypedFunction<Message> keyFunction = keyFunctions.functions().get(0).function();
                final TypedFunction<Message> valueFunction = valueFunctions.functions().get(0).function();
                return namedField(ToObjectFunction.of(message -> {
                    final Map<Object, Object> map = new HashMap<>();
                    final int count = message.getRepeatedFieldCount(fd);
                    for (int i = 0; i < count; ++i) {
                        final Message obj = (Message) message.getRepeatedField(fd, i);
                        final Object key = Box.apply(keyFunction, obj);
                        final Object value = Box.apply(valueFunction, obj);
                        map.put(key, value);
                    }
                    return map;
                }, Type.ofCustom(Map.class)));
            }

            private ProtobufFunctions delegate() {
                return new RepeatedFieldObject().functions();
            }
        }

        private class RepeatedFieldObject {

            private ProtobufFunctions functions() {
                switch (fd.getJavaType()) {
                    case INT:
                        return namedField(mapInts(ToIntFunction.cast()));
                    case LONG:
                        return namedField(mapLongs(ToLongFunction.cast()));
                    case FLOAT:
                        return namedField(mapFloats(ToFloatFunction.cast()));
                    case DOUBLE:
                        return namedField(mapDoubles(ToDoubleFunction.cast()));
                    case BOOLEAN:
                        return namedField(mapBooleans(ToBooleanFunction.cast()));
                    case STRING:
                        return namedField(mapGenerics(STRING_OBJ));
                    case BYTE_STRING:
                        return options.fieldOptions().apply(fieldPath).bytes() == BytesBehavior.asByteArray()
                                ? namedField(mapGenerics(BYTE_STRING_OBJ.mapToObj(BYTE_STRING_FUNCTION)))
                                : namedField(mapGenerics(BYTE_STRING_OBJ));
                    case ENUM:
                        return namedField(mapGenerics(ENUM_VALUE_DESCRIPTOR_OBJ));
                    case MESSAGE: {
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions functions = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();
                        for (ProtobufFunction f : functions.functions()) {
                            final ToObjectFunction<Message, ?> repeatedTf = f.function().walk(new ToRepeatedType());
                            builder.addFunctions(ProtobufFunction.of(prepend(f.path(), fd), repeatedTf));
                        }
                        return builder.build();
                    }
                    default:
                        throw new IllegalStateException();
                }
            }

            private <T> ToObjectFunction<Message, T> maybeBypass(ToObjectFunction<Message, T> f) {
                // Ideally, we could be very targetted in our application of null checks; in a lot of contexts, our
                // implementation could know it will never be called with a null message to produce an array.
                return BypassOnNull.of(f);
            }

            private ToObjectFunction<Message, char[]> mapChars(ToCharFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toChars(m, fd, f), Type.charType().arrayType()));
            }

            private ToObjectFunction<Message, byte[]> mapBytes(ToByteFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toBytes(m, fd, f), Type.byteType().arrayType()));
            }

            private ToObjectFunction<Message, short[]> mapShorts(ToShortFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toShorts(m, fd, f), Type.shortType().arrayType()));
            }

            private ToObjectFunction<Message, int[]> mapInts(ToIntFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toInts(m, fd, f), Type.intType().arrayType()));
            }

            private ToObjectFunction<Message, long[]> mapLongs(ToLongFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toLongs(m, fd, f), Type.longType().arrayType()));
            }

            private ToObjectFunction<Message, float[]> mapFloats(ToFloatFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toFloats(m, fd, f), Type.floatType().arrayType()));
            }

            private ToObjectFunction<Message, double[]> mapDoubles(ToDoubleFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toDoubles(m, fd, f), Type.doubleType().arrayType()));
            }

            private ToObjectFunction<Message, boolean[]> mapBooleans(ToBooleanFunction<Object> f) {
                return maybeBypass(ToObjectFunction.of(m -> toBooleans(m, fd, f), Type.booleanType().arrayType()));
            }

            private <T> ToObjectFunction<Message, T[]> mapGenerics(ToObjectFunction<Object, T> f) {
                return maybeBypass(ToObjectFunction.of(message -> toArray(message, fd, f), f.returnType().arrayType()));
            }

            private class ToRepeatedType implements
                    Visitor<Message, ToObjectFunction<Message, ?>>,
                    ToPrimitiveFunction.Visitor<Message, ToObjectFunction<Message, ?>> {

                @Override
                public ToObjectFunction<Message, ?> visit(ToObjectFunction<Message, ?> f) {
                    return mapGenerics(MESSAGE_OBJ.mapToObj(f));
                }

                @Override
                public ToObjectFunction<Message, ?> visit(ToPrimitiveFunction<Message> f) {
                    return f.walk((ToPrimitiveFunction.Visitor<Message, ToObjectFunction<Message, ?>>) this);
                }

                @Override
                public ToObjectFunction<Message, boolean[]> visit(ToBooleanFunction<Message> f) {
                    return mapBooleans(MESSAGE_OBJ.mapToBoolean(f));
                }

                @Override
                public ToObjectFunction<Message, char[]> visit(ToCharFunction<Message> f) {
                    return mapChars(MESSAGE_OBJ.mapToChar(f));
                }

                @Override
                public ToObjectFunction<Message, byte[]> visit(ToByteFunction<Message> f) {
                    return mapBytes(MESSAGE_OBJ.mapToByte(f));
                }

                @Override
                public ToObjectFunction<Message, short[]> visit(ToShortFunction<Message> f) {
                    return mapShorts(MESSAGE_OBJ.mapToShort(f));
                }

                @Override
                public ToObjectFunction<Message, int[]> visit(ToIntFunction<Message> f) {
                    return mapInts(MESSAGE_OBJ.mapToInt(f));
                }

                @Override
                public ToObjectFunction<Message, long[]> visit(ToLongFunction<Message> f) {
                    return mapLongs(MESSAGE_OBJ.mapToLong(f));
                }

                @Override
                public ToObjectFunction<Message, float[]> visit(ToFloatFunction<Message> f) {
                    return mapFloats(MESSAGE_OBJ.mapToFloat(f));
                }

                @Override
                public ToObjectFunction<Message, double[]> visit(ToDoubleFunction<Message> f) {
                    return mapDoubles(MESSAGE_OBJ.mapToDouble(f));
                }
            }

            private ProtobufFunctions namedField(TypedFunction<Message> tf) {
                return ProtobufFunctions.of(ProtobufFunction.of(FieldPath.of(fd), tf));
            }
        }
    }

    private static char[] toChars(Message message, FieldDescriptor fd, ToCharFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final char[] array = new char[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsChar(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static byte[] toBytes(Message message, FieldDescriptor fd, ToByteFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final byte[] array = new byte[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsByte(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static short[] toShorts(Message message, FieldDescriptor fd, ToShortFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final short[] array = new short[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsShort(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static int[] toInts(Message message, FieldDescriptor fd, ToIntFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final int[] array = new int[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsInt(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static long[] toLongs(Message message, FieldDescriptor fd, ToLongFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final long[] array = new long[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsLong(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static float[] toFloats(Message message, FieldDescriptor fd, ToFloatFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final float[] array = new float[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsFloat(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static double[] toDoubles(Message message, FieldDescriptor fd, ToDoubleFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final double[] array = new double[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsDouble(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static boolean[] toBooleans(Message message, FieldDescriptor fd, ToBooleanFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final boolean[] array = new boolean[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.test(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static <T> T[] toArray(Message message, FieldDescriptor fd, ToObjectFunction<Object, T> f) {
        final int count = message.getRepeatedFieldCount(fd);
        // noinspection unchecked
        final T[] array = (T[]) Array.newInstance(f.returnType().clazz(), count);
        for (int i = 0; i < count; ++i) {
            array[i] = f.apply(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static FieldPath prepend(FieldPath f, FieldDescriptor fd) {
        return FieldPath.of(Stream.concat(Stream.of(fd), f.path().stream()).collect(Collectors.toList()));
    }

    private static FieldPath append(FieldPath f, FieldDescriptor fd) {
        return FieldPath.of(Stream.concat(f.path().stream(), Stream.of(fd)).collect(Collectors.toList()));
    }
}
