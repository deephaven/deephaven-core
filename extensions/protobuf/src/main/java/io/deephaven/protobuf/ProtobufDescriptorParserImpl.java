/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import com.google.protobuf.Message;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.Type;
import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.PrimitiveFunction;
import io.deephaven.functions.ShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.functions.TypedFunction.Visitor;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

class ProtobufDescriptorParserImpl {

    private static final ObjectFunction<Object, String> STRING_OBJ = ObjectFunction.identity(Type.stringType());
    private static final ObjectFunction<Object, Integer> BOXED_INT_OBJ = ObjectFunction.identity(BoxedIntType.of());
    private static final ObjectFunction<Object, Long> BOXED_LONG_OBJ = ObjectFunction.identity(BoxedLongType.of());
    private static final ObjectFunction<Object, Float> BOXED_FLOAT_OBJ = ObjectFunction.identity(BoxedFloatType.of());
    private static final ObjectFunction<Object, Double> BOXED_DOUBLE_OBJ =
            ObjectFunction.identity(BoxedDoubleType.of());
    private static final ObjectFunction<Object, Boolean> BOXED_BOOLEAN_OBJ =
            ObjectFunction.identity(BoxedBooleanType.of());
    private static final ObjectFunction<Object, Message> MESSAGE_OBJ =
            ObjectFunction.identity(Type.ofCustom(Message.class));
    private static final ObjectFunction<Object, ByteString> BYTE_STRING_OBJ =
            ObjectFunction.identity(Type.ofCustom(ByteString.class));
    private static final ObjectFunction<Object, EnumValueDescriptor> ENUM_VALUE_DESCRIPTOR_OBJ =
            ObjectFunction.identity(Type.ofCustom(EnumValueDescriptor.class));
    private static final ObjectFunction<ByteString, byte[]> BYTE_STRING_FUNCTION =
            BypassOnNull.of(ObjectFunction.of(ByteString::toByteArray, Type.byteType().arrayType()));

    private final ProtobufDescriptorParserOptions options;
    private final Map<String, SingleValuedMessageParser> byFullName;

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
            // todo: eventually support cases that are >1 field
            final SingleValuedMessageParser svmp = byFullName.get(descriptor.getFullName());
            if (svmp == null) {
                return Optional.empty();
            }
            if (!options.parseAsWellKnown().test(fieldPath)) {
                return Optional.empty();
            }
            return Optional.of(ProtobufFunctions.unnamed(svmp.messageParser(descriptor, options)));
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
            this.fieldPath = parent.fieldPath.append(fd);
        }

        private ProtobufFunctions functions() {
            return functions(false);
        }

        private ProtobufFunctions functions(boolean forceInclude) {
            if (!forceInclude && !options.include().test(fieldPath)) {
                return ProtobufFunctions.empty();
            }
            final ProtobufFunctions wellKnown = wellKnown().orElse(null);
            if (wellKnown != null && options.parseAsWellKnown().test(fieldPath)) {
                return wellKnown;
            }
            if (fd.isMapField() && options.parseAsMap().test(fieldPath)) {
                return new MapFieldObject().functions();
            }
            if (fd.isRepeated()) {
                return new RepeatedFieldObject().functions();
            }
            return new FieldObject().functions();
        }

        private Optional<ProtobufFunctions> wellKnown() {
            // todo: eventually have support for parsing specific fields in specific ways
            return Optional.empty();
        }

        private ProtobufFunctions namedField(TypedFunction<Message> tf) {
            // todo: can we re-work this, so we can use fieldPath here instead and not use prefix later?
            return ProtobufFunctions.of(ProtobufFunction.of(FieldPath.of(fd), tf));
        }

        private DescriptorContext toMessageContext() {
            if (fd.getJavaType() != JavaType.MESSAGE) {
                throw new IllegalStateException();
            }
            return new DescriptorContext(fieldPath, fd.getMessageType());
        }

        private class FieldObject implements ObjectFunction<Message, Object> {

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
                // 2) protobuf already has the null object, so it doesn't harm us to propogate it to the calling layer,
                // and for the calling layer to unbox if desired.
                switch (fd.getJavaType()) {
                    case INT:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_INT_OBJ))
                                : namedField(mapInt(IntFunction.primitive()));
                    case LONG:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_LONG_OBJ))
                                : namedField(mapLong(LongFunction.primitive()));
                    case FLOAT:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_FLOAT_OBJ))
                                : namedField(mapFloat(FloatFunction.primitive()));
                    case DOUBLE:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_DOUBLE_OBJ))
                                : namedField(mapDouble(DoubleFunction.primitive()));
                    case BOOLEAN:
                        return fd.hasPresence()
                                ? namedField(mapObj(BOXED_BOOLEAN_OBJ))
                                : namedField(mapBoolean(BooleanFunction.primitive()));
                    case STRING:
                        return namedField(mapObj(STRING_OBJ));
                    case BYTE_STRING:
                        return options.parseAsBytes().test(fieldPath)
                                ? namedField(mapObj(BYTE_STRING_OBJ).mapObj(BYTE_STRING_FUNCTION))
                                : namedField(mapObj(BYTE_STRING_OBJ));
                    case ENUM:
                        return namedField(mapObj(ENUM_VALUE_DESCRIPTOR_OBJ));
                    case MESSAGE: {
                        final ObjectFunction<Message, Message> fieldAsMessage = mapObj(MESSAGE_OBJ);
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
                                    ProtobufFunction.of(e.path().prefixWith(fd), value.mapInput(fieldAsMessage)));
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
                // message MapFieldEntry {
                // option map_entry = true;
                // optional KeyType key = 1;
                // optional ValueType value = 2;
                // }
                // repeated MapFieldEntry map_field = 1;

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
                final DescriptorContext dc = new DescriptorContext(parent.fieldPath.append(fd), fd.getMessageType());

                // Note: maps are a "special" case, where even though we don't include the key / value FDs as a return
                // io.deephaven.protobuf.ProtobufFunction#path, it's important that we force their inclusion if we've
                // gotten this far.
                //
                // For example, if we have the schema:
                // message MyMessage {
                // map<int32, int32> my_map = 1;
                // }
                //
                // The user should be able to use:
                // ProtobufDescriptorParserOptions.builder()
                // .include(FieldPath.namePathEquals(List.of("my_map")))
                // .build()
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
                return namedField(ObjectFunction.of(message -> {
                    final Map<Object, Object> map = new HashMap<>();
                    final int count = message.getRepeatedFieldCount(fd);
                    for (int i = 0; i < count; ++i) {
                        final Message obj = (Message) message.getRepeatedField(fd, i);
                        final Object key = UpcastApply.apply(keyFunction, obj);
                        final Object value = UpcastApply.apply(valueFunction, obj);
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
                        return namedField(mapInts(IntFunction.primitive()));
                    case LONG:
                        return namedField(mapLongs(LongFunction.primitive()));
                    case FLOAT:
                        return namedField(mapFloats(FloatFunction.primitive()));
                    case DOUBLE:
                        return namedField(mapDoubles(DoubleFunction.primitive()));
                    case BOOLEAN:
                        return namedField(mapBooleans(BooleanFunction.primitive()));
                    case STRING:
                        return namedField(mapGenerics(STRING_OBJ));
                    case BYTE_STRING:
                        return options.parseAsBytes().test(fieldPath)
                                ? namedField(mapGenerics(BYTE_STRING_OBJ.mapObj(BYTE_STRING_FUNCTION)))
                                : namedField(mapGenerics(BYTE_STRING_OBJ));
                    case ENUM:
                        return namedField(mapGenerics(ENUM_VALUE_DESCRIPTOR_OBJ));
                    case MESSAGE: {
                        final DescriptorContext messageContext = toMessageContext();
                        final ProtobufFunctions functions = messageContext.functions();
                        final Builder builder = ProtobufFunctions.builder();
                        for (ProtobufFunction f : functions.functions()) {
                            final ObjectFunction<Message, ?> repeatedTf = f.function().walk(new ToRepeatedType());
                            builder.addFunctions(ProtobufFunction.of(f.path().prefixWith(fd), repeatedTf));
                        }
                        return builder.build();
                    }
                    default:
                        throw new IllegalStateException();
                }
            }

            private ObjectFunction<Message, char[]> mapChars(CharFunction<Object> f) {
                return ObjectFunction.of(m -> toChars(m, fd, f), Type.charType().arrayType());
            }

            private ObjectFunction<Message, byte[]> mapBytes(ByteFunction<Object> f) {
                return ObjectFunction.of(m -> toBytes(m, fd, f), Type.byteType().arrayType());
            }

            private ObjectFunction<Message, short[]> mapShorts(ShortFunction<Object> f) {
                return ObjectFunction.of(m -> toShorts(m, fd, f), Type.shortType().arrayType());
            }

            private ObjectFunction<Message, int[]> mapInts(IntFunction<Object> f) {
                return ObjectFunction.of(m -> toInts(m, fd, f), Type.intType().arrayType());
            }

            private ObjectFunction<Message, long[]> mapLongs(LongFunction<Object> f) {
                return ObjectFunction.of(m -> toLongs(m, fd, f), Type.longType().arrayType());
            }

            private ObjectFunction<Message, float[]> mapFloats(FloatFunction<Object> f) {
                return ObjectFunction.of(m -> toFloats(m, fd, f), Type.floatType().arrayType());
            }

            private ObjectFunction<Message, double[]> mapDoubles(DoubleFunction<Object> f) {
                return ObjectFunction.of(m -> toDoubles(m, fd, f), Type.doubleType().arrayType());
            }

            private ObjectFunction<Message, boolean[]> mapBooleans(BooleanFunction<Object> f) {
                return ObjectFunction.of(m -> toBooleans(m, fd, f), Type.booleanType().arrayType());
            }

            private <T> ObjectFunction<Message, T[]> mapGenerics(ObjectFunction<Object, T> f) {
                return ObjectFunction.of(message -> toArray(message, fd, f), f.returnType().arrayType());
            }

            private class ToRepeatedType implements
                    Visitor<Message, ObjectFunction<Message, ?>>,
                    PrimitiveFunction.Visitor<Message, ObjectFunction<Message, ?>> {

                @Override
                public ObjectFunction<Message, ?> visit(ObjectFunction<Message, ?> f) {
                    return mapGenerics(MESSAGE_OBJ.mapObj(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(PrimitiveFunction<Message> f) {
                    return f.walk((PrimitiveFunction.Visitor<Message, ObjectFunction<Message, ?>>) this);
                }

                @Override
                public ObjectFunction<Message, ?> visit(BooleanFunction<Message> f) {
                    return mapBooleans(MESSAGE_OBJ.mapBoolean(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(CharFunction<Message> f) {
                    return mapChars(MESSAGE_OBJ.mapChar(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(ByteFunction<Message> f) {
                    return mapBytes(MESSAGE_OBJ.mapByte(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(ShortFunction<Message> f) {
                    return mapShorts(MESSAGE_OBJ.mapShort(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(IntFunction<Message> f) {
                    return mapInts(MESSAGE_OBJ.mapInt(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(LongFunction<Message> f) {
                    return mapLongs(MESSAGE_OBJ.mapLong(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(FloatFunction<Message> f) {
                    return mapFloats(MESSAGE_OBJ.mapFloat(f));
                }

                @Override
                public ObjectFunction<Message, ?> visit(DoubleFunction<Message> f) {
                    return mapDoubles(MESSAGE_OBJ.mapDouble(f));
                }
            }

            private ProtobufFunctions namedField(TypedFunction<Message> tf) {
                return ProtobufFunctions.of(ProtobufFunction.of(FieldPath.of(fd), tf));
            }
        }
    }

    private static char[] toChars(Message message, FieldDescriptor fd, CharFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final char[] array = new char[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsChar(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static byte[] toBytes(Message message, FieldDescriptor fd, ByteFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final byte[] array = new byte[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsByte(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static short[] toShorts(Message message, FieldDescriptor fd, ShortFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final short[] array = new short[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsShort(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static int[] toInts(Message message, FieldDescriptor fd, IntFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final int[] array = new int[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsInt(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static long[] toLongs(Message message, FieldDescriptor fd, LongFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final long[] array = new long[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsLong(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static float[] toFloats(Message message, FieldDescriptor fd, FloatFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final float[] array = new float[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsFloat(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static double[] toDoubles(Message message, FieldDescriptor fd, DoubleFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final double[] array = new double[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.applyAsDouble(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static boolean[] toBooleans(Message message, FieldDescriptor fd, BooleanFunction<Object> f) {
        final int count = message.getRepeatedFieldCount(fd);
        final boolean[] array = new boolean[count];
        for (int i = 0; i < count; ++i) {
            array[i] = f.test(message.getRepeatedField(fd, i));
        }
        return array;
    }

    private static <T> T[] toArray(Message message, FieldDescriptor fd, ObjectFunction<Object, T> f) {
        final int count = message.getRepeatedFieldCount(fd);
        // noinspection unchecked
        final T[] array = (T[]) Array.newInstance(f.returnType().clazz(), count);
        for (int i = 0; i < count; ++i) {
            array[i] = f.apply(message.getRepeatedField(fd, i));
        }
        return array;
    }
}
