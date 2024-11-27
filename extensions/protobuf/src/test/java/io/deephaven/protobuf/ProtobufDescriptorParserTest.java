//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.protobuf;

import com.google.protobuf.Any;
import com.google.protobuf.BoolValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumValueDescriptor;
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
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.protobuf.FieldOptions.BytesBehavior;
import io.deephaven.protobuf.FieldOptions.MapBehavior;
import io.deephaven.protobuf.FieldOptions.WellKnownBehavior;
import io.deephaven.protobuf.test.ADuration;
import io.deephaven.protobuf.test.ALongNestedTimestampMap;
import io.deephaven.protobuf.test.ALongTimestampMap;
import io.deephaven.protobuf.test.AMultiNested;
import io.deephaven.protobuf.test.AMultiNested.SubMessage1;
import io.deephaven.protobuf.test.AMultiNested.SubMessage1.SubMessage2;
import io.deephaven.protobuf.test.ANested;
import io.deephaven.protobuf.test.ANested.SubMessage;
import io.deephaven.protobuf.test.AStringStringMap;
import io.deephaven.protobuf.test.ATimestamp;
import io.deephaven.protobuf.test.AnEnum;
import io.deephaven.protobuf.test.AnEnum.TheEnum;
import io.deephaven.protobuf.test.AnIntFooBarMap;
import io.deephaven.protobuf.test.AnIntFooBarMap.FooBar;
import io.deephaven.protobuf.test.AnIntIntMap;
import io.deephaven.protobuf.test.AnyWrapper;
import io.deephaven.protobuf.test.ByteWrapper;
import io.deephaven.protobuf.test.ByteWrapperRepeated;
import io.deephaven.protobuf.test.FieldMaskWrapper;
import io.deephaven.protobuf.test.MultiRepeated;
import io.deephaven.protobuf.test.NestedArrays;
import io.deephaven.protobuf.test.NestedByteWrapper;
import io.deephaven.protobuf.test.NestedRepeatedTimestamps;
import io.deephaven.protobuf.test.NestedRepeatedTimestamps.Timestamps;
import io.deephaven.protobuf.test.OptionalBasics;
import io.deephaven.protobuf.test.RepeatedBasics;
import io.deephaven.protobuf.test.RepeatedDuration;
import io.deephaven.protobuf.test.RepeatedMessage;
import io.deephaven.protobuf.test.RepeatedMessage.Person;
import io.deephaven.protobuf.test.RepeatedTimestamp;
import io.deephaven.protobuf.test.RepeatedWrappers;
import io.deephaven.protobuf.test.TheWrappers;
import io.deephaven.protobuf.test.TwoTs;
import io.deephaven.protobuf.test.UnionType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.QueryConstants;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

public class ProtobufDescriptorParserTest {

    @Test
    public void string() {
        checkKey(StringValue.getDescriptor(), List.of(), Type.stringType(), Map.of(
                StringValue.of("foo"), "foo",
                StringValue.of("bar"), "bar"));
    }

    @Test
    public void int32() {
        checkKey(Int32Value.getDescriptor(), List.of(), Type.intType(), Map.of(
                Int32Value.of(42), 42,
                Int32Value.of(43), 43));
    }

    @Test
    public void uint32() {
        checkKey(UInt32Value.getDescriptor(), List.of(), Type.intType(), Map.of(
                UInt32Value.of(42), 42,
                UInt32Value.of(43), 43));
    }

    @Test
    public void int64() {
        checkKey(Int64Value.getDescriptor(), List.of(), Type.longType(), Map.of(
                Int64Value.of(42), 42L,
                Int64Value.of(43), 43L));
    }

    @Test
    public void uint64() {
        checkKey(UInt64Value.getDescriptor(), List.of(), Type.longType(), Map.of(
                UInt64Value.of(42), 42L,
                UInt64Value.of(43), 43L));
    }

    @Test
    public void float_() {
        checkKey(FloatValue.getDescriptor(), List.of(), Type.floatType(), Map.of(
                FloatValue.of(42), 42.0f,
                FloatValue.of(43), 43.0f));
    }

    @Test
    public void double_() {
        checkKey(DoubleValue.getDescriptor(), List.of(), Type.doubleType(), Map.of(
                DoubleValue.of(42), 42.0d,
                DoubleValue.of(43), 43.0d));
    }

    @Test
    public void bool() {
        checkKey(BoolValue.getDescriptor(), List.of(), Type.booleanType(), Map.of(
                BoolValue.of(true), true,
                BoolValue.of(false), false));
    }

    @Test
    public void bytes() {
        final ByteString foo = ByteString.copyFromUtf8("foo");
        final ByteString bar = ByteString.copyFromUtf8("bar");
        checkKey(BytesValue.getDescriptor(), List.of(), Type.byteType().arrayType(), Map.of(
                BytesValue.of(foo), "foo".getBytes(StandardCharsets.UTF_8),
                BytesValue.of(bar), "bar".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void timestamp() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(Timestamp.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of());
        checkKey(
                Timestamp.getDescriptor(),
                List.of(),
                Type.instantType(),
                Map.of(
                        Timestamp.getDefaultInstance(),
                        Instant.ofEpochSecond(0),
                        Timestamp.newBuilder().setSeconds(1).setNanos(2).build(),
                        Instant.ofEpochSecond(1, 2)));
    }

    @Test
    void unionTypes() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));
    }

    @Test
    void oneOfBool() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setBool(true).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isEqualTo(true);
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfInt32() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setInt32(42).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isEqualTo(42);
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfUInt32() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setUint32(42).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isEqualTo(42);
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfInt64() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setInt64(42).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isEqualTo(42L);
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfUInt64() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setUint64(42).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isEqualTo(42L);
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfFloat() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setFloat(42.0f).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isEqualTo(42.0f);
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfDouble() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setDouble(42.0d).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isEqualTo(42.0d);
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfString() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setString("hello").build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isEqualTo("hello");
        assertThat(Box.apply(nf.get(List.of("bytes")), message)).isNull();
    }

    @Test
    void oneOfBytes() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(UnionType.getDescriptor());
        final UnionType message = UnionType.newBuilder().setBytes(ByteString.copyFromUtf8("world")).build();
        assertThat(Box.apply(nf.get(List.of("bool")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint32")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("int64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("uint64")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("float")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("double")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("string")), message)).isNull();
        assertThat(Box.apply(nf.get(List.of("bytes")), message))
                .isEqualTo("world".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void stringStringMap() {
        checkKey(AStringStringMap.getDescriptor(), List.of("properties"), Type.ofCustom(Map.class),
                Map.of(AStringStringMap.newBuilder()
                        .putProperties("foo", "bar")
                        .putProperties("hello", "world").build(),
                        Map.of("foo", "bar", "hello", "world")));
    }

    @Test
    void intIntMap() {
        checkKey(AnIntIntMap.getDescriptor(), List.of("properties"), Type.ofCustom(Map.class),
                Map.of(AnIntIntMap.newBuilder()
                        .putProperties(1, 2)
                        .putProperties(3, 4).build(),
                        Map.of(1, 2, 3, 4)));
    }

    @Test
    void intIntMapRestrictiveInclude() {
        final List<String> path = List.of("properties");
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(includeIf(fp -> fp.namePath().equals(path)))
                .build();
        checkKey(AnIntIntMap.getDescriptor(), options, path, Type.ofCustom(Map.class),
                Map.of(AnIntIntMap.newBuilder()
                        .putProperties(1, 2)
                        .putProperties(3, 4).build(),
                        Map.of(1, 2, 3, 4)));
    }

    @Test
    void longTimestampMap() {
        checkKey(ALongTimestampMap.getDescriptor(), List.of("properties"), Type.ofCustom(Map.class),
                Map.of(ALongTimestampMap.newBuilder()
                        .putProperties(1, Timestamp.newBuilder().setSeconds(42).build())
                        .putProperties(2, Timestamp.newBuilder().setSeconds(43).build()).build(),
                        Map.of(1L, Instant.ofEpochSecond(42),
                                2L, Instant.ofEpochSecond(43))));
    }

    @Test
    void longNestedTimestampMap() {
        checkKey(ALongNestedTimestampMap.getDescriptor(), List.of("properties"), Type.ofCustom(Map.class),
                Map.of(ALongNestedTimestampMap.newBuilder()
                        .putProperties(1,
                                ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(42).build()).build())
                        .putProperties(2,
                                ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(43).build()).build())
                        .build(),
                        Map.of(1L, Instant.ofEpochSecond(42),
                                2L, Instant.ofEpochSecond(43))));
    }

    @Test
    void intFooBarMap() {
        // this test is exercising the case where the value is a complex type, and we default back to treating it as a
        // repeating type instead of a map
        final AnIntFooBarMap empty = AnIntFooBarMap.getDefaultInstance();
        final AnIntFooBarMap map1 = AnIntFooBarMap.newBuilder()
                .putProperties(1, FooBar.newBuilder().build())
                .build();
        final AnIntFooBarMap map2 = AnIntFooBarMap.newBuilder()
                .putProperties(1, FooBar.newBuilder().build())
                .putProperties(2, FooBar.newBuilder().setFoo("fighter").setBar(42).build())
                .build();

        checkKey(
                AnIntFooBarMap.getDescriptor(),
                List.of("properties", "key"),
                Type.intType().arrayType(),
                Map.of(
                        empty, new int[0],
                        map1, new int[] {1},
                        map2, new int[] {1, 2}));

        checkKey(
                AnIntFooBarMap.getDescriptor(),
                List.of("properties", "value", "foo"),
                Type.stringType().arrayType(),
                Map.of(
                        empty, new String[0],
                        map1, new String[] {""},
                        map2, new String[] {"", "fighter"}));

        checkKey(
                AnIntFooBarMap.getDescriptor(),
                List.of("properties", "value", "bar"),
                Type.longType().arrayType(),
                Map.of(
                        empty, new long[0],
                        map1, new long[] {0L},
                        map2, new long[] {0L, 42L}));
    }

    @Test
    void longTimestampMapAsRepeated() {
        final FieldOptions antiMap = FieldOptions.builder().map(MapBehavior.asRepeated()).build();
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(x -> antiMap)
                .build();
        final ALongTimestampMap e1 = ALongTimestampMap.newBuilder()
                .putProperties(1, Timestamp.newBuilder().setSeconds(42).build())
                .putProperties(2, Timestamp.newBuilder().setSeconds(43).build())
                .build();
        checkKey(ALongTimestampMap.getDescriptor(),
                options,
                List.of("properties", "key"),
                Type.longType().arrayType(),
                Map.of(e1, new long[] {1, 2}));
        checkKey(ALongTimestampMap.getDescriptor(),
                options,
                List.of("properties", "value"),
                Type.instantType().arrayType(),
                Map.of(e1, new Instant[] {Instant.ofEpochSecond(42), Instant.ofEpochSecond(43)}));
    }

    @Test
    void aTimestamp() {
        checkKey(ATimestamp.getDescriptor(), List.of("ts"), Type.instantType(), Map.of(
                ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(42).setNanos(43).build())
                        .build(),
                Instant.ofEpochSecond(42, 43)));
    }

    @Test
    void aDuration() {
        checkKey(ADuration.getDescriptor(), List.of("dur"), Type.ofCustom(Duration.class), Map.of(
                ADuration.newBuilder()
                        .setDur(com.google.protobuf.Duration.newBuilder().setSeconds(4200).setNanos(4300)
                                .build())
                        .build(),
                Duration.ofSeconds(4200, 4300)));
    }

    @Test
    void enum_() {
        checkKey(AnEnum.getDescriptor(), List.of("fbb"), Type.ofCustom(EnumValueDescriptor.class), Map.of(
                AnEnum.newBuilder().setFbb(TheEnum.FOO).build(), TheEnum.FOO.getValueDescriptor(),
                AnEnum.newBuilder().setFbb(TheEnum.BAR).build(), TheEnum.BAR.getValueDescriptor(),
                AnEnum.newBuilder().setFbb(TheEnum.BAZ).build(), TheEnum.BAZ.getValueDescriptor(),
                AnEnum.newBuilder().setFbbValue(999).build(),
                TheEnum.getDescriptor().findValueByNumberCreatingIfUnknown(999)));
    }

    @Test
    void optionalBasics() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(OptionalBasics.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("bool"),
                BoxedBooleanType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setBool(false).build(), false);
                        put(OptionalBasics.newBuilder().setBool(true).build(), true);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("int32"),
                BoxedIntType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setInt32(0).build(), 0);
                        put(OptionalBasics.newBuilder().setInt32(42).build(), 42);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("uint32"),
                BoxedIntType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setUint32(0).build(), 0);
                        put(OptionalBasics.newBuilder().setUint32(42).build(), 42);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("int64"),
                BoxedLongType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setInt64(0).build(), 0L);
                        put(OptionalBasics.newBuilder().setInt64(42).build(), 42L);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("uint64"),
                BoxedLongType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setUint64(0).build(), 0L);
                        put(OptionalBasics.newBuilder().setUint64(42).build(), 42L);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("float"),
                BoxedFloatType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setFloat(0).build(), 0f);
                        put(OptionalBasics.newBuilder().setFloat(42).build(), 42f);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("double"),
                BoxedDoubleType.of(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setDouble(0).build(), 0d);
                        put(OptionalBasics.newBuilder().setDouble(42).build(), 42d);
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("string"),
                Type.stringType(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setString("").build(), "");
                        put(OptionalBasics.newBuilder().setString("hello").build(), "hello");
                    }
                });

        checkKey(
                OptionalBasics.getDescriptor(),
                List.of("bytes"),
                Type.byteType().arrayType(),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setBytes(ByteString.EMPTY).build(), new byte[0]);
                        put(OptionalBasics.newBuilder().setBytes(ByteString.copyFromUtf8("hello")).build(),
                                "hello".getBytes(StandardCharsets.UTF_8));
                    }
                });
    }

    @Test
    void optionalBasicsByteString() {
        final FieldOptions asByteString = FieldOptions.builder().bytes(BytesBehavior.asByteString()).build();
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(x -> asByteString)
                .build();
        checkKey(
                OptionalBasics.getDescriptor(),
                options,
                List.of("bytes"),
                CustomType.of(ByteString.class),
                new HashMap<>() {
                    {
                        put(OptionalBasics.getDefaultInstance(), null);
                        put(OptionalBasics.newBuilder().setBytes(ByteString.EMPTY).build(), ByteString.EMPTY);
                        final ByteString hello = ByteString.copyFromUtf8("hello");
                        put(OptionalBasics.newBuilder().setBytes(hello).build(), hello);
                    }
                });
    }

    @Test
    void wrappers() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(TheWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        TheWrappers allNull = TheWrappers.getDefaultInstance();
        checkKey(TheWrappers.getDescriptor(), List.of("bool"), BoxedBooleanType.of(), new HashMap<>() {
            {
                put(allNull, null);
                put(TheWrappers.newBuilder().setBool(BoolValue.newBuilder().setValue(true).build()).build(), true);
            }
        });

        checkKey(TheWrappers.getDescriptor(), List.of("int32"), Type.intType(),
                new HashMap<>() {
                    {
                        put(allNull, QueryConstants.NULL_INT);
                        put(TheWrappers.newBuilder().setInt32(Int32Value.newBuilder().setValue(42).build()).build(),
                                42);
                    }
                });

        checkKey(TheWrappers.getDescriptor(), List.of("uint32"), Type.intType(),
                new HashMap<>() {
                    {
                        put(allNull, QueryConstants.NULL_INT);
                        put(TheWrappers.newBuilder().setUint32(UInt32Value.newBuilder().setValue(42).build()).build(),
                                42);
                    }
                });

        checkKey(TheWrappers.getDescriptor(), List.of("int64"), Type.longType(),
                new HashMap<>() {
                    {
                        put(allNull, QueryConstants.NULL_LONG);
                        put(TheWrappers.newBuilder().setInt64(Int64Value.newBuilder().setValue(42).build()).build(),
                                42L);
                    }
                });

        checkKey(TheWrappers.getDescriptor(), List.of("uint64"), Type.longType(),
                new HashMap<>() {
                    {
                        put(allNull, QueryConstants.NULL_LONG);
                        put(TheWrappers.newBuilder().setUint64(UInt64Value.newBuilder().setValue(42).build()).build(),
                                42L);
                    }
                });

        checkKey(TheWrappers.getDescriptor(), List.of("float"), Type.floatType(),
                new HashMap<>() {
                    {
                        put(allNull, QueryConstants.NULL_FLOAT);
                        put(TheWrappers.newBuilder().setFloat(FloatValue.newBuilder().setValue(42).build()).build(),
                                42.0f);
                    }
                });

        checkKey(TheWrappers.getDescriptor(), List.of("double"), Type.doubleType(),
                new HashMap<>() {
                    {
                        put(allNull, QueryConstants.NULL_DOUBLE);
                        put(TheWrappers.newBuilder().setDouble(DoubleValue.newBuilder().setValue(42).build()).build(),
                                42.0d);
                    }
                });

        checkKey(TheWrappers.getDescriptor(), List.of("string"), Type.stringType(), new HashMap<>() {
            {
                put(allNull, null);
                put(TheWrappers.newBuilder().setString(StringValue.newBuilder().setValue("foo").build())
                        .build(), "foo");
            }
        });

        {
            final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                    .fieldOptions(fp -> FieldOptions.builder().bytes(BytesBehavior.asByteString()).build())
                    .build();
            checkKey(TheWrappers.getDescriptor(), options, List.of("bytes"), Type.ofCustom(ByteString.class),
                    new HashMap<>() {
                        {
                            put(allNull, null);
                            final ByteString foo = ByteString.copyFromUtf8("foo");
                            put(TheWrappers.newBuilder().setBytes(BytesValue.newBuilder().setValue(foo).build())
                                    .build(), foo);
                        }
                    });
        }
    }



    @Test
    void repeated() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(RepeatedBasics.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        final RepeatedBasics allEmpty = RepeatedBasics.getDefaultInstance();

        checkKey(RepeatedBasics.getDescriptor(), List.of("bool"), Type.booleanType().arrayType(), new HashMap<>() {
            {
                put(allEmpty, new boolean[] {});
                put(RepeatedBasics.newBuilder().addBool(true).addBool(false).build(), new boolean[] {true, false});
            }
        });

        checkKey(RepeatedBasics.getDescriptor(), List.of("int32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedBasics.newBuilder().addInt32(42).addInt32(43).build(), new int[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("uint32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedBasics.newBuilder().addUint32(42).addUint32(43).build(), new int[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("int64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedBasics.newBuilder().addInt64(42).addInt64(43).build(), new long[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("uint64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedBasics.newBuilder().addUint64(42).addUint64(43).build(), new long[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("float"), Type.floatType().arrayType(), Map.of(
                allEmpty, new float[] {},
                RepeatedBasics.newBuilder().addFloat(42).addFloat(43).build(), new float[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("double"), Type.doubleType().arrayType(), Map.of(
                allEmpty, new double[] {},
                RepeatedBasics.newBuilder().addDouble(42).addDouble(43).build(),
                new double[] {42, 43}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("string"), Type.stringType().arrayType(), Map.of(
                allEmpty, new String[] {},
                RepeatedBasics.newBuilder().addString("foo").addString("bar").build(),
                new String[] {"foo", "bar"}));

        checkKey(RepeatedBasics.getDescriptor(), List.of("bytes"), Type.byteType().arrayType().arrayType(), Map.of(
                allEmpty, new byte[][] {},
                RepeatedBasics.newBuilder().addBytes(ByteString.copyFromUtf8("hello"))
                        .addBytes(ByteString.copyFromUtf8("foo")).build(),
                new byte[][] {"hello".getBytes(StandardCharsets.UTF_8), "foo".getBytes(StandardCharsets.UTF_8)}));
    }

    @Test
    void repeatedByteString() {
        final FieldOptions asByteString = FieldOptions.builder().bytes(BytesBehavior.asByteString()).build();
        final RepeatedBasics allEmpty = RepeatedBasics.getDefaultInstance();
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(x -> asByteString)
                .build();
        final ByteString hello = ByteString.copyFromUtf8("hello");
        final ByteString foo = ByteString.copyFromUtf8("foo");
        checkKey(RepeatedBasics.getDescriptor(),
                options,
                List.of("bytes"),
                CustomType.of(ByteString.class).arrayType(),
                Map.of(
                        allEmpty, new ByteString[] {},
                        RepeatedBasics.newBuilder().addBytes(hello).addBytes(foo).build(),
                        new ByteString[] {hello, foo}));
    }

    @Test
    void repeatedWrappers() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(RepeatedWrappers.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("bool"),
                List.of("int32"),
                List.of("uint32"),
                List.of("int64"),
                List.of("uint64"),
                List.of("float"),
                List.of("double"),
                List.of("string"),
                List.of("bytes"));

        final RepeatedWrappers allEmpty = RepeatedWrappers.getDefaultInstance();

        checkKey(RepeatedWrappers.getDescriptor(), List.of("bool"), Type.booleanType().arrayType(), Map.of(
                allEmpty, new boolean[] {},
                RepeatedWrappers.newBuilder()
                        .addBool(BoolValue.newBuilder().build())
                        .addBool(BoolValue.newBuilder().setValue(false).build())
                        .addBool(BoolValue.newBuilder().setValue(true).build())
                        .build(),
                new boolean[] {false, false, true}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("int32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedWrappers.newBuilder().addInt32(Int32Value.of(42)).addInt32(Int32Value.of(43)).build(),
                new int[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("uint32"), Type.intType().arrayType(), Map.of(
                allEmpty, new int[] {},
                RepeatedWrappers.newBuilder().addUint32(UInt32Value.of(42)).addUint32(UInt32Value.of(43))
                        .build(),
                new int[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("int64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedWrappers.newBuilder().addInt64(Int64Value.of(42)).addInt64(Int64Value.of(43)).build(),
                new long[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("uint64"), Type.longType().arrayType(), Map.of(
                allEmpty, new long[] {},
                RepeatedWrappers.newBuilder().addUint64(UInt64Value.of(42)).addUint64(UInt64Value.of(43))
                        .build(),
                new long[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("float"), Type.floatType().arrayType(), Map.of(
                allEmpty, new float[] {},
                RepeatedWrappers.newBuilder().addFloat(FloatValue.of(42)).addFloat(FloatValue.of(43)).build(),
                new float[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("double"), Type.doubleType().arrayType(), Map.of(
                allEmpty, new double[] {},
                RepeatedWrappers.newBuilder().addDouble(DoubleValue.of(42)).addDouble(DoubleValue.of(43))
                        .build(),
                new double[] {42, 43}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("string"), Type.stringType().arrayType(), Map.of(
                allEmpty, new String[] {},
                RepeatedWrappers.newBuilder().addString(StringValue.of("foo")).addString(StringValue.of("bar")).build(),
                new String[] {"foo", "bar"}));

        checkKey(RepeatedWrappers.getDescriptor(), List.of("bytes"), Type.byteType().arrayType().arrayType(), Map.of(
                allEmpty, new byte[][] {},
                RepeatedWrappers.newBuilder().addBytes(BytesValue.of(ByteString.copyFromUtf8("hello")))
                        .addBytes(BytesValue.of(ByteString.copyFromUtf8("foo"))).build(),
                new byte[][] {"hello".getBytes(StandardCharsets.UTF_8), "foo".getBytes(StandardCharsets.UTF_8)}));
    }

    @Test
    void multiRepeated() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(MultiRepeated.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("my_basics", "bool"),
                List.of("my_basics", "int32"),
                List.of("my_basics", "uint32"),
                List.of("my_basics", "int64"),
                List.of("my_basics", "uint64"),
                List.of("my_basics", "float"),
                List.of("my_basics", "double"),
                List.of("my_basics", "string"),
                List.of("my_basics", "bytes"),
                List.of("my_wrappers", "bool"),
                List.of("my_wrappers", "int32"),
                List.of("my_wrappers", "uint32"),
                List.of("my_wrappers", "int64"),
                List.of("my_wrappers", "uint64"),
                List.of("my_wrappers", "float"),
                List.of("my_wrappers", "double"),
                List.of("my_wrappers", "string"),
                List.of("my_wrappers", "bytes"),
                List.of("my_objects", "xyz", "x"),
                List.of("my_objects", "xyz", "y"),
                List.of("my_objects", "xyz", "z"));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "bool"),
                Type.booleanType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new boolean[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "int32"),
                Type.intType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new int[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "uint32"),
                Type.intType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new int[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "int64"),
                Type.longType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new long[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "uint64"),
                Type.longType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new long[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "float"),
                Type.floatType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new float[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_basics", "double"),
                Type.doubleType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new double[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "bool"),
                Type.booleanType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new boolean[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "int32"),
                Type.intType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new int[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "uint32"),
                Type.intType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new int[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "int64"),
                Type.longType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new long[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "uint64"),
                Type.longType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new long[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "float"),
                Type.floatType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new float[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_wrappers", "double"),
                Type.doubleType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new double[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_objects", "xyz", "x"),
                Type.intType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new int[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_objects", "xyz", "y"),
                Type.stringType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new String[][] {}));

        checkKey(
                MultiRepeated.getDescriptor(),
                List.of("my_objects", "xyz", "z"),
                Type.instantType().arrayType().arrayType(),
                Map.of(MultiRepeated.getDefaultInstance(), new Instant[][] {}));
    }

    @Test
    void repeatedTimestamp() {
        checkKey(
                RepeatedTimestamp.getDescriptor(),
                List.of("ts"),
                Type.instantType().arrayType(),
                Map.of(
                        RepeatedTimestamp.getDefaultInstance(), new Instant[] {},
                        RepeatedTimestamp.newBuilder()
                                .addTs(Timestamp.newBuilder().setSeconds(1).setNanos(2).build())
                                .addTs(Timestamp.newBuilder().setSeconds(3).setNanos(4).build())
                                .build(),
                        new Instant[] {
                                Instant.ofEpochSecond(1, 2),
                                Instant.ofEpochSecond(3, 4)}));
    }

    @Test
    void repeatedDuration() {
        checkKey(RepeatedDuration.getDescriptor(), List.of("dur"), Type.ofCustom(Duration.class).arrayType(), Map.of(
                RepeatedDuration.getDefaultInstance(), new Duration[] {},
                RepeatedDuration.newBuilder()
                        .addDur(com.google.protobuf.Duration.newBuilder().setSeconds(1).setNanos(2).build())
                        .addDur(com.google.protobuf.Duration.newBuilder().setSeconds(3).setNanos(4).build())
                        .build(),
                new Duration[] {
                        Duration.ofSeconds(1, 2),
                        Duration.ofSeconds(3, 4)}));
    }

    @Test
    void nested() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(ANested.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("baz", "foo"), List.of("baz", "bar"));

        checkKey(
                ANested.getDescriptor(),
                List.of("baz", "foo"),
                Type.intType(),
                new HashMap<>() {
                    {
                        put(ANested.getDefaultInstance(), QueryConstants.NULL_INT);
                        put(ANested.newBuilder().setBaz(SubMessage.newBuilder().setFoo(42).build()).build(), 42);
                    }
                });

        checkKey(
                ANested.getDescriptor(),
                List.of("baz", "bar"),
                Type.longType(),
                new HashMap<>() {
                    {
                        put(ANested.getDefaultInstance(), QueryConstants.NULL_LONG);
                        put(ANested.newBuilder().setBaz(SubMessage.newBuilder().setBar(42L).build()).build(), 42L);
                    }
                });
    }

    @Test
    void multiNested() {
        final Map<List<String>, TypedFunction<Message>> nf =
                nf(AMultiNested.getDescriptor());
        assertThat(nf.keySet()).containsExactly(
                List.of("hello", "foo"),
                List.of("hello", "bar"),
                List.of("hello", "baz", "world"),
                List.of("hello", "baz", "world2"),
                List.of("hello", "baz", "world3"));

        final AMultiNested defaultInstance = AMultiNested.getDefaultInstance();
        final AMultiNested noBaz = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setFoo(42).setBar(43).build())
                .build();
        final AMultiNested bazDefault = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setBaz(SubMessage2.getDefaultInstance()).build())
                .build();
        final AMultiNested bazWorld = AMultiNested.newBuilder()
                .setHello(SubMessage1.newBuilder().setBaz(SubMessage2.newBuilder()
                        .setWorld("OK")
                        .setWorld2(StringValue.newBuilder().setValue("OK2"))
                        .setWorld3(DoubleValue.newBuilder().setValue(42.0d)).build())
                        .build())
                .build();

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "foo"),
                Type.intType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, QueryConstants.NULL_INT);
                        put(noBaz, 42);
                        put(bazDefault, 0);
                        put(bazWorld, 0);
                    }
                });

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "bar"),
                Type.longType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, QueryConstants.NULL_LONG);
                        put(noBaz, 43L);
                        put(bazDefault, 0L);
                        put(bazWorld, 0L);
                    }
                });

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "baz", "world"),
                Type.stringType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, null);
                        put(noBaz, null);
                        put(bazDefault, "");
                        put(bazWorld, "OK");
                    }
                });

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "baz", "world2"),
                Type.stringType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, null);
                        put(noBaz, null);
                        put(bazDefault, null);
                        put(bazWorld, "OK2");
                    }
                });

        checkKey(
                AMultiNested.getDescriptor(),
                List.of("hello", "baz", "world3"),
                Type.doubleType(),
                new HashMap<>() {
                    {
                        put(defaultInstance, QueryConstants.NULL_DOUBLE);
                        put(noBaz, QueryConstants.NULL_DOUBLE);
                        put(bazDefault, QueryConstants.NULL_DOUBLE);
                        put(bazWorld, 42.0d);
                    }
                });
    }

    @Test
    void repeatedPerson() {
        final MessageParser personParser = new MessageParserSingle() {
            @Override
            public Descriptor canonicalDescriptor() {
                return Person.getDescriptor();
            }

            @Override
            public ToObjectFunction<Message, Person> messageParser(Descriptor descriptor,
                    ProtobufDescriptorParserOptions options, FieldPath fieldPath) {
                return ToObjectFunction.identity(Type.ofCustom(Person.class));
            }
        };
        final ProtobufDescriptorParserOptions options =
                ProtobufDescriptorParserOptions.builder().parsers(List.of(personParser)).build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(RepeatedMessage.getDescriptor(), options);
        assertThat(nf.keySet()).containsExactly(List.of("persons"));

        final Person p1 = Person.newBuilder().setFirstName("First").setLastName("Last").build();
        final Person p2 = Person.newBuilder().setFirstName("Foo").setLastName("Bar").build();
        checkKey(
                RepeatedMessage.getDescriptor(),
                options,
                List.of("persons"),
                Type.ofCustom(Person.class).arrayType(),
                Map.of(
                        RepeatedMessage.getDefaultInstance(), new Person[] {},
                        RepeatedMessage.newBuilder().addPersons(p1).addPersons(p2).build(), new Person[] {p1, p2}));
    }

    @Test
    void customMultiParser() {
        final MessageParser parser = new MessageParser() {
            @Override
            public Descriptor canonicalDescriptor() {
                return Timestamp.getDescriptor();
            }

            @Override
            public ProtobufFunctions messageParsers(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                    FieldPath fieldPath) {
                final ToObjectFunction<Message, Timestamp> ts =
                        ToObjectFunction.identity(Type.ofCustom(Timestamp.class));
                // This is a dumb thing to do; but it shows that we can derive two functions from the same field path
                final ToLongFunction<Message> seconds = ts.mapToLong(Timestamp::getSeconds);
                final ToLongFunction<Message> secondsX2 = ts.mapToLong(x -> x.getSeconds() * 2);
                final FieldPath secondsPath =
                        FieldPath.of(descriptor.findFieldByNumber(Timestamp.SECONDS_FIELD_NUMBER));
                return ProtobufFunctions.builder()
                        .addFunctions(
                                ProtobufFunction.of(secondsPath, seconds),
                                ProtobufFunction.of(secondsPath, secondsX2))
                        .build();
            }
        };

        final ProtobufDescriptorParserOptions options =
                ProtobufDescriptorParserOptions.builder().parsers(List.of(parser)).build();

        final ProtobufFunctions pf = ProtobufDescriptorParser.parse(Timestamp.getDescriptor(), options);
        final List<ProtobufFunction> functions = pf.functions();
        assertThat(functions).hasSize(2);

        assertThat(functions.get(0).path().namePath()).containsExactly("seconds");
        assertThat(functions.get(1).path().namePath()).containsExactly("seconds");

        final ToLongFunction<Message> f0 = (ToLongFunction<Message>) functions.get(0).function();
        final ToLongFunction<Message> f1 = (ToLongFunction<Message>) functions.get(1).function();

        final Timestamp aTs = Timestamp.newBuilder().setSeconds(42).build();
        assertThat(f0.applyAsLong(aTs)).isEqualTo(42);
        assertThat(f1.applyAsLong(aTs)).isEqualTo(84);

        assertThat(f0.applyAsLong(Timestamp.getDefaultInstance())).isEqualTo(0);
        assertThat(f1.applyAsLong(Timestamp.getDefaultInstance())).isEqualTo(0);
    }

    @Test
    void customMultiParserNested() {
        final MessageParser parser = new MessageParser() {
            @Override
            public Descriptor canonicalDescriptor() {
                return Timestamp.getDescriptor();
            }

            @Override
            public ProtobufFunctions messageParsers(Descriptor descriptor, ProtobufDescriptorParserOptions options,
                    FieldPath fieldPath) {
                final ToObjectFunction<Message, Timestamp> ts =
                        ToObjectFunction.identity(Type.ofCustom(Timestamp.class));
                // This is a dumb thing to do; but it shows that we can derive two functions from the same field path
                final ToLongFunction<Message> seconds = ts.mapToLong(Timestamp::getSeconds);
                final ToLongFunction<Message> secondsX2 = ts.mapToLong(x -> x.getSeconds() * 2);
                final FieldPath secondsPath =
                        FieldPath.of(descriptor.findFieldByNumber(Timestamp.SECONDS_FIELD_NUMBER));
                return ProtobufFunctions.builder()
                        .addFunctions(
                                ProtobufFunction.of(secondsPath, seconds),
                                ProtobufFunction.of(secondsPath, secondsX2))
                        .build();
            }
        };

        final ProtobufDescriptorParserOptions options =
                ProtobufDescriptorParserOptions.builder().parsers(List.of(parser)).build();

        final ProtobufFunctions pf = ProtobufDescriptorParser.parse(ATimestamp.getDescriptor(), options);
        final List<ProtobufFunction> functions = pf.functions();
        assertThat(functions).hasSize(2);

        assertThat(functions.get(0).path().namePath()).containsExactly("ts", "seconds");
        assertThat(functions.get(1).path().namePath()).containsExactly("ts", "seconds");

        final ToLongFunction<Message> f0 = (ToLongFunction<Message>) functions.get(0).function();
        final ToLongFunction<Message> f1 = (ToLongFunction<Message>) functions.get(1).function();

        final ATimestamp aTs = ATimestamp.newBuilder().setTs(Timestamp.newBuilder().setSeconds(42).build()).build();

        assertThat(f0.applyAsLong(aTs)).isEqualTo(42);
        assertThat(f1.applyAsLong(aTs)).isEqualTo(84);

        assertThat(f0.applyAsLong(ATimestamp.getDefaultInstance())).isEqualTo(QueryConstants.NULL_LONG);
        assertThat(f1.applyAsLong(ATimestamp.getDefaultInstance())).isEqualTo(QueryConstants.NULL_LONG);
    }

    // This is a potential improvement in parsing we might want in the future
    @Test
    void repeatedMessageDestructured() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(
                RepeatedMessage.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("persons", "first_name"), List.of("persons", "last_name"));

        final Person p1 = Person.newBuilder().setFirstName("First").setLastName("Last").build();
        final Person p2 = Person.newBuilder().setFirstName("Foo").setLastName("Bar").build();

        checkKey(
                RepeatedMessage.getDescriptor(),
                List.of("persons", "first_name"),
                Type.stringType().arrayType(),
                Map.of(
                        RepeatedMessage.getDefaultInstance(), new String[] {},
                        RepeatedMessage.newBuilder().addPersons(p1).addPersons(p2).build(),
                        new String[] {"First", "Foo"}));

        checkKey(
                RepeatedMessage.getDescriptor(),
                List.of("persons", "last_name"),
                Type.stringType().arrayType(),
                Map.of(
                        RepeatedMessage.getDefaultInstance(), new String[] {},
                        RepeatedMessage.newBuilder().addPersons(p1).addPersons(p2).build(),
                        new String[] {"Last", "Bar"}));
    }

    @Test
    void nestedRepeatedTimestamps() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(NestedRepeatedTimestamps.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("stamps", "ts"));
        checkKey(
                NestedRepeatedTimestamps.getDescriptor(),
                List.of("stamps", "ts"),
                Type.instantType().arrayType().arrayType(),
                Map.of(
                        NestedRepeatedTimestamps.getDefaultInstance(), new Instant[][] {},
                        NestedRepeatedTimestamps.newBuilder().addStamps(Timestamps.getDefaultInstance()).build(),
                        new Instant[][] {new Instant[] {}},
                        NestedRepeatedTimestamps.newBuilder()
                                .addStamps(Timestamps.newBuilder().addTs(Timestamp.getDefaultInstance()).build())
                                .build(),
                        new Instant[][] {new Instant[] {Instant.ofEpochMilli(0)}}));

    }

    @Test
    void byteWrapper() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(ByteWrapper.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of());
        checkKey(
                ByteWrapper.getDescriptor(),
                List.of(),
                Type.byteType(),
                Map.of(
                        ByteWrapper.getDefaultInstance(), (byte) 0,
                        ByteWrapper.newBuilder().setValue(42).build(), (byte) 42));
    }

    @Test
    void byteWrapperNested() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(NestedByteWrapper.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("my_byte"));
        checkKey(
                NestedByteWrapper.getDescriptor(),
                List.of("my_byte"),
                Type.byteType(),
                new HashMap<>() {
                    {
                        put(NestedByteWrapper.getDefaultInstance(), QueryConstants.NULL_BYTE);
                        put(NestedByteWrapper.newBuilder().setMyByte(ByteWrapper.getDefaultInstance()).build(),
                                (byte) 0);
                        put(NestedByteWrapper.newBuilder().setMyByte(ByteWrapper.newBuilder().setValue(42)).build(),
                                (byte) 42);
                    }
                });
    }

    @Test
    void repeatedByteWrapper() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(ByteWrapperRepeated.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("my_bytes"));
        checkKey(
                ByteWrapperRepeated.getDescriptor(),
                List.of("my_bytes"),
                Type.byteType().arrayType(),
                Map.of(
                        ByteWrapperRepeated.getDefaultInstance(), new byte[0],
                        ByteWrapperRepeated.newBuilder().addMyBytes(ByteWrapper.newBuilder().setValue(42).build())
                                .build(),
                        new byte[] {(byte) 42}));
    }

    @Test
    void any() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(AnyWrapper.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("my_any"));
        checkKey(
                AnyWrapper.getDescriptor(),
                List.of("my_any"),
                Type.ofCustom(Any.class),
                new HashMap<>() {
                    {
                        put(AnyWrapper.getDefaultInstance(), null);
                        put(AnyWrapper.newBuilder().setMyAny(Any.getDefaultInstance()).build(),
                                Any.getDefaultInstance());
                        final Any someAny = Any.newBuilder().setTypeUrl("some-url").build();
                        put(AnyWrapper.newBuilder().setMyAny(someAny).build(), someAny);
                    }
                });
    }

    @Test
    void fieldMask() {
        final Map<List<String>, TypedFunction<Message>> nf = nf(FieldMaskWrapper.getDescriptor());
        assertThat(nf.keySet()).containsExactly(List.of("my_field_mask"));
        checkKey(
                FieldMaskWrapper.getDescriptor(),
                List.of("my_field_mask"),
                Type.ofCustom(FieldMask.class),
                new HashMap<>() {
                    {
                        put(FieldMaskWrapper.getDefaultInstance(), null);
                        put(FieldMaskWrapper.newBuilder()
                                .setMyFieldMask(FieldMask.getDefaultInstance()).build(),
                                FieldMask.getDefaultInstance());
                        final FieldMask someFieldMask = FieldMask.newBuilder().addPaths("foo").build();
                        put(FieldMaskWrapper.newBuilder().setMyFieldMask(someFieldMask).build(), someFieldMask);
                    }
                });
    }

    @Test
    void includeNamePaths() {
        final List<String> path = List.of("float");
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(includeIf(fp -> fp.namePath().equals(path)))
                .build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(UnionType.getDescriptor(), options);
        assertThat(nf.keySet()).containsExactly(path);
    }

    @Test
    void includeNumberPaths() {
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(includeIf(fp -> fp.numberPath().equals(FieldNumberPath.of(6))))
                .build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(UnionType.getDescriptor(), options);
        assertThat(nf.keySet()).containsExactly(List.of("float"));
    }

    @Test
    void excludeNamePaths() {
        final List<String> path = List.of("float");
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(includeIf(fp -> !fp.namePath().equals(path)))
                .build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(UnionType.getDescriptor(), options);
        assertThat(nf).hasSize(8);
        assertThat(nf.keySet()).doesNotContain(path);
    }

    @Test
    void excludeNumberPaths() {
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(includeIf(fp -> !fp.numberPath().equals(FieldNumberPath.of(6))))
                .build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(UnionType.getDescriptor(), options);
        assertThat(nf).hasSize(8);
        assertThat(nf.keySet()).doesNotContain(List.of("float"));
    }

    @Test
    void timestampNoParsers() {
        final ProtobufDescriptorParserOptions options =
                ProtobufDescriptorParserOptions.builder().parsers(List.of()).build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(Timestamp.getDescriptor(), options);
        assertThat(nf.keySet()).containsExactly(
                List.of("seconds"),
                List.of("nanos"));
    }

    @Test
    void twoTimestampsOneAsWellKnown() {
        // only treat t1 as well-known
        final ProtobufDescriptorParserOptions options = ProtobufDescriptorParserOptions.builder()
                .fieldOptions(fieldPath -> FieldOptions.builder()
                        .wellKnown(fieldPath.namePath().equals(List.of("ts1")) ? WellKnownBehavior.asWellKnown()
                                : WellKnownBehavior.asRecursive())
                        .build())
                .build();
        final Map<List<String>, TypedFunction<Message>> nf = nf(TwoTs.getDescriptor(), options);
        assertThat(nf.keySet()).containsExactly(List.of("ts1"), List.of("ts2", "seconds"), List.of("ts2", "nanos"));
    }

    @Test
    void nestedArraysADirect() {
        checkKey(
                NestedArrays.getDescriptor(),
                List.of("a_direct", "b", "c"),
                Type.stringType().arrayType(),
                new HashMap<>() {
                    {
                        put(NestedArrays.getDefaultInstance(), null);

                        put(NestedArrays.newBuilder()
                                .setADirect(NestedArrays.A.getDefaultInstance())
                                .build(), null);

                        // c is only non-null when b has been explicitly set

                        put(NestedArrays.newBuilder()
                                .setADirect(NestedArrays.A.newBuilder()
                                        .setB(NestedArrays.B.getDefaultInstance())
                                        .build())
                                .build(), new String[0]);

                        put(NestedArrays.newBuilder()
                                .setADirect(NestedArrays.A.newBuilder()
                                        .setB(NestedArrays.B.newBuilder()
                                                .addC("Foo")
                                                .addC("Bar")
                                                .build())
                                        .build())
                                .build(), new String[] {"Foo", "Bar"});
                    }
                });
    }

    @Test
    void nestedArraysARepeated() {
        checkKey(
                NestedArrays.getDescriptor(),
                List.of("a_repeated", "b", "c"),
                Type.stringType().arrayType().arrayType(),
                new HashMap<>() {
                    {
                        put(NestedArrays.getDefaultInstance(), new String[0][]);
                        put(NestedArrays.newBuilder()
                                .addARepeated(NestedArrays.A.getDefaultInstance())
                                .addARepeated(NestedArrays.A.newBuilder()
                                        .setB(NestedArrays.B.getDefaultInstance())
                                        .build())
                                .addARepeated(NestedArrays.A.newBuilder()
                                        .setB(NestedArrays.B.newBuilder()
                                                .addC("Foo")
                                                .addC("Bar")
                                                .build())
                                        .build())
                                .build(), new String[][] {null, new String[0], new String[] {"Foo", "Bar"}});
                    }
                });
    }

    @Test
    void nestedArraysBDirect() {
        checkKey(
                NestedArrays.getDescriptor(),
                List.of("b_direct", "c"),
                Type.stringType().arrayType(),
                new HashMap<>() {
                    {
                        put(NestedArrays.getDefaultInstance(), null);

                        put(NestedArrays.newBuilder()
                                .setBDirect(NestedArrays.B.getDefaultInstance())
                                .build(), new String[0]);

                        put(NestedArrays.newBuilder()
                                .setBDirect(NestedArrays.B.newBuilder()
                                        .addC("Foo")
                                        .addC("Bar")
                                        .build())
                                .build(), new String[] {"Foo", "Bar"});
                    }
                });
    }

    @Test
    void nestedArraysBRepeated() {
        checkKey(
                NestedArrays.getDescriptor(),
                List.of("b_repeated", "c"),
                Type.stringType().arrayType().arrayType(),
                new HashMap<>() {
                    {
                        put(NestedArrays.getDefaultInstance(), new String[0][]);

                        put(NestedArrays.newBuilder()
                                .addBRepeated(NestedArrays.B.getDefaultInstance())
                                .addBRepeated(NestedArrays.B.newBuilder()
                                        .addC("Foo")
                                        .addC("Bar")
                                        .build())

                                .build(), new String[][] {new String[0], new String[] {"Foo", "Bar"}});
                    }
                });
    }

    private static Map<List<String>, TypedFunction<Message>> nf(Descriptor descriptor) {
        return nf(descriptor, ProtobufDescriptorParserOptions.defaults());
    }

    private static Map<List<String>, TypedFunction<Message>> nf(Descriptor descriptor,
            ProtobufDescriptorParserOptions options) {
        final ProtobufFunctions results = ProtobufDescriptorParser.parse(descriptor, options);
        final Map<List<String>, TypedFunction<Message>> out = new LinkedHashMap<>(results.functions().size());
        for (ProtobufFunction function : results.functions()) {
            out.put(function.path().namePath(), function.function());
        }
        return out;
    }

    private static <T> void checkKey(
            Descriptor descriptor,
            List<String> expectedPath,
            Type<T> expectedType,
            Map<Message, T> expectedExamples) {
        checkKey(descriptor, ProtobufDescriptorParserOptions.defaults(), expectedPath, expectedType, expectedExamples);
    }

    private static <T> void checkKey(
            Descriptor descriptor,
            ProtobufDescriptorParserOptions options,
            List<String> expectedPath,
            Type<T> expectedType,
            Map<Message, T> expectedExamples) {
        final Map<List<String>, TypedFunction<Message>> map = nf(descriptor, options);
        assertThat(map).containsKey(expectedPath);
        assertThat(map)
                .extractingByKey(expectedPath)
                .extracting(TypedFunction::returnType)
                .isEqualTo(expectedType);
        for (Entry<Message, T> e : expectedExamples.entrySet()) {
            assertThat(map)
                    .extractingByKey(expectedPath)
                    .extracting(t -> Box.apply(t, e.getKey()))
                    .isEqualTo(e.getValue());
        }
    }

    private static Function<FieldPath, FieldOptions> includeIf(Predicate<FieldPath> include) {
        return fp -> FieldOptions.builder().include(include.test(fp)).build();
    }
}
