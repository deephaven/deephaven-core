//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.ByteString;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.qst.type.Type;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingConsumer;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.UnionMode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.LongStream;

public class BarrageColumnRoundTripTest extends RefreshingTableTestCase {

    private static final String DH_TYPE_TAG = BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_TYPE_TAG;
    private static final String DH_COMPONENT_TYPE_TAG =
            BarrageUtil.ATTR_DH_PREFIX + BarrageUtil.ATTR_COMPONENT_TYPE_TAG;
    private static final int FIXED_LIST_LEN = 4;
    private static final int MAX_LIST_LEN = 10;

    private static final BarrageSubscriptionOptions OPT_DEFAULT = BarrageSubscriptionOptions.builder()
            .build();
    private static final BarrageSubscriptionOptions OPT_DH_NULLS =
            BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(true)
                    .build();
    private static final BarrageSubscriptionOptions OPT_COLUMNS_AS_LIST = BarrageSubscriptionOptions.builder()
            .columnsAsList(true)
            .build();

    private static final BarrageSubscriptionOptions[] OPTIONS = new BarrageSubscriptionOptions[] {
            OPT_DEFAULT,
            OPT_DH_NULLS,
            OPT_COLUMNS_AS_LIST,
    };

    private static WritableChunk<Values> readChunk(
            final BarrageOptions options,
            final Class<?> type,
            final Class<?> componentType,
            final Field field,
            final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int offset,
            final int totalRows) throws IOException {
        return DefaultChunkReaderFactory.INSTANCE
                .newReader(BarrageTypeInfo.make(type, componentType, field), options)
                .readChunk(fieldNodeIter, bufferInfoIter, is, outChunk, offset, totalRows);
    }

    public void testDenseUnionChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.DENSE_UNION, opts, Object.class, (utO) -> {
                final WritableObjectChunk<Object, Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    if (i % 7 == 0) {
                        chunk.set(i, null);
                    } else if (i % 2 == 0) {
                        chunk.set(i, (long) random.nextInt(1000));
                    } else {
                        chunk.set(i, "value" + random.nextInt(1000));
                    }
                }
            }, (utO, utC, subset, offset) -> {
                final WritableObjectChunk<Object, Values> original = utO.asWritableObjectChunk();
                final WritableObjectChunk<Object, Values> computed = utC.asWritableObjectChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); i++) {
                        final Object orig = original.get(i);
                        final Object comp = computed.get(offset + i);
                        Assert.nullSafeEquals(orig, "orig", comp, "comp");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> {
                        final Object orig = original.get((int) key);
                        final Object comp = computed.get(offset + off.getAndIncrement());
                        Assert.nullSafeEquals(orig, "orig", comp, "comp");
                    });
                }
            });
        }
    }

    public void testSparseUnionChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.SPARSE_UNION, opts, Object.class, (utO) -> {
                final WritableObjectChunk<Object, Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    if (i % 7 == 0) {
                        chunk.set(i, null);
                    } else if (i % 2 == 0) {
                        chunk.set(i, (long) random.nextInt(1000));
                    } else {
                        chunk.set(i, "value" + random.nextInt(1000));
                    }
                }
            }, (utO, utC, subset, offset) -> {
                final WritableObjectChunk<Object, Values> original = utO.asWritableObjectChunk();
                final WritableObjectChunk<Object, Values> computed = utC.asWritableObjectChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); i++) {
                        final Object orig = original.get(i);
                        final Object comp = computed.get(offset + i);
                        Assert.nullSafeEquals(orig, "orig", comp, "comp");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> {
                        final Object orig = original.get((int) key);
                        final Object comp = computed.get(offset + off.getAndIncrement());
                        Assert.nullSafeEquals(orig, "orig", comp, "comp");
                    });
                }
            });
        }
    }

    public void testMapChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.MAP, opts, Map.class, (utO) -> {
                final WritableObjectChunk<Map<String, String>, Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    if (i % 7 == 0) {
                        chunk.set(i, null);
                    } else {
                        final Map<String, String> map = new LinkedHashMap<>();
                        final int entryCount = random.nextInt(MAX_LIST_LEN) + 1;
                        for (int j = 0; j < entryCount; j++) {
                            map.put("key" + j, "value" + random.nextInt(1000));
                        }
                        chunk.set(i, map);
                    }
                }
            }, (utO, utC, subset, offset) -> {
                final WritableObjectChunk<Map<String, String>, Values> original = utO.asWritableObjectChunk();
                final WritableObjectChunk<Map<String, String>, Values> computed = utC.asWritableObjectChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); i++) {
                        final Map<String, String> orig = original.get(i);
                        final Map<String, String> comp = computed.get(offset + i);
                        assertMapEquals(orig, comp, "map compare");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> {
                        final Map<String, String> orig = original.get((int) key);
                        final Map<String, String> comp = computed.get(offset + off.getAndIncrement());
                        assertMapEquals(orig, comp, "map compare");
                    });
                }
            });
        }
    }

    /**
     * Compares two maps for equality.
     * <p>
     * If both maps are {@code null}, they are considered equal. If one is {@code null} and the other is not, the
     * assertion will fail. Otherwise, the method checks that both maps have the same size and that each key/value pair
     * matches.
     *
     * @param expected the expected map
     * @param actual the actual map
     * @param context a description of the current comparison context for error reporting
     */
    private void assertMapEquals(Map<String, String> expected, Map<String, String> actual, String context) {
        if (expected == null && actual == null) {
            return;
        }
        if (expected == null || actual == null) {
            Assert.statementNeverExecuted(
                    context + ": One of the maps is null - expected: " + expected + ", actual: " + actual);
        }
        if (expected.size() != actual.size()) {
            Assert.statementNeverExecuted(
                    context + ": Map sizes differ - expected: " + expected.size() + ", actual: " + actual.size());
        }
        for (String key : expected.keySet()) {
            if (!actual.containsKey(key)) {
                Assert.statementNeverExecuted(
                        context + ": Missing key '" + key + "' in actual map. Expected map: " + expected);
            }
            final String expectedValue = expected.get(key);
            final String actualValue = actual.get(key);
            if (!Objects.equals(expectedValue, actualValue)) {
                Assert.statementNeverExecuted(context + ": Value mismatch for key '" + key + "' - expected: "
                        + expectedValue + ", actual: " + actualValue);
            }
        }
    }

    public void testVarLenListChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.VAR_LEN_LIST, opts, String[].class, (utO) -> {
                final WritableObjectChunk<String[], Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    if (i % 7 == 0) {
                        chunk.set(i, null);
                    } else {
                        final int rowLen = random.nextInt(MAX_LIST_LEN) + 1;
                        final String[] list = new String[rowLen];
                        for (int j = 0; j < rowLen; j++) {
                            list[j] = "value" + random.nextInt(1000);
                        }
                        chunk.set(i, list);
                    }
                }
            }, (utO, utC, subset, offset) -> {
                final WritableObjectChunk<String[], Values> original = utO.asWritableObjectChunk();
                final WritableObjectChunk<String[], Values> computed = utC.asWritableObjectChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); i++) {
                        final String[] orig = original.get(i);
                        final String[] comp = computed.get(offset + i);
                        assertListEquals(orig, comp, "list compare at index " + i);
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> {
                        final String[] orig = original.get((int) key);
                        final String[] comp = computed.get(offset + off.getAndIncrement());
                        assertListEquals(orig, comp, "list compare for row key " + key);
                    });
                }
            });
        }
    }

    public void testFixedLenListChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.FIXED_LEN_LIST, opts, String[].class, (utO) -> {
                final WritableObjectChunk<String[], Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    if (i % 7 == 0) {
                        chunk.set(i, null);
                    } else {
                        final int rowLen = random.nextInt(MAX_LIST_LEN) + 1;
                        final String[] list = new String[rowLen];
                        for (int j = 0; j < rowLen; j++) {
                            list[j] = "value" + random.nextInt(1000);
                        }
                        chunk.set(i, list);
                    }
                }
            }, (utO, utC, subset, offset) -> {
                final WritableObjectChunk<String[], Values> original = utO.asWritableObjectChunk();
                final WritableObjectChunk<String[], Values> computed = utC.asWritableObjectChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); i++) {
                        final String[] orig = original.get(i);
                        final String[] comp = computed.get(offset + i);
                        assertListEquals(FIXED_LIST_LEN, orig, comp, "list compare at index " + i);
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> {
                        final String[] orig = original.get((int) key);
                        final String[] comp = computed.get(offset + off.getAndIncrement());
                        assertListEquals(FIXED_LIST_LEN, orig, comp, "list compare for row key " + key);
                    });
                }
            });
        }
    }

    /**
     * Compares two lists of strings for equality.
     *
     * @param expected the expected list
     * @param actual the actual list
     * @param context a description of the current comparison context for error reporting
     */
    private static void assertListEquals(String[] expected, String[] actual, String context) {
        assertListEquals(expected == null ? -1 : expected.length, expected, actual, context);
    }

    /**
     * Compares two lists of strings for equality.
     *
     * @param expected the expected list
     * @param actual the actual list
     * @param context a description of the current comparison context for error reporting
     */
    private static void assertListEquals(int length, String[] expected, String[] actual, String context) {
        if (expected == null && actual == null) {
            return;
        }
        if (expected == null || actual == null) {
            Assert.statementNeverExecuted(
                    context + ": One of the lists is null - expected: " + expected + ", actual: " + actual);
        }
        if (length != actual.length) {
            Assert.statementNeverExecuted(
                    context + ": List sizes differ - expected: " + length + ", actual: " + actual.length);
        }
        for (int ii = 0; ii < Math.min(length, expected.length); ii++) {
            final String expectedStr = expected[ii];
            final String actualStr = actual[ii];
            if (!Objects.equals(expectedStr, actualStr)) {
                Assert.statementNeverExecuted(context + ": Mismatch at index " + ii + " - expected: " + expectedStr
                        + ", actual: " + actualStr);
            }
        }
        for (int ii = expected.length; ii < length; ++ii) {
            Assert.eqNull(actual[ii], "actual[i]");
        }
    }

    public void testCharChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, char.class, (utO) -> {
                final WritableCharChunk<Values> chunk = utO.asWritableCharChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_CHAR : (char) random.nextInt());
                }
            }, (utO, utC, subset, offset) -> {
                final WritableCharChunk<Values> original = utO.asWritableCharChunk();
                final WritableCharChunk<Values> computed = utC.asWritableCharChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testBooleanChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, boolean.class, (utO) -> {
                final WritableByteChunk<Values> chunk = utO.asWritableByteChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, BooleanUtils.booleanAsByte(i % 7 == 0 ? null : random.nextBoolean()));
                }
            }, (utO, utC, subset, offset) -> {
                final WritableByteChunk<Values> original = utO.asWritableByteChunk();
                final WritableByteChunk<Values> computed = utC.asWritableByteChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testBooleanChunkSerializationNonStandardNulls() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, boolean.class, (utO) -> {
                final WritableByteChunk<Values> chunk = utO.asWritableByteChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, (byte) i);
                }
            }, (utO, utC, subset, offset) -> {
                final WritableByteChunk<Values> original = utO.asWritableByteChunk();
                final WritableByteChunk<Values> computed = utC.asWritableByteChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Boolean origBoolean = BooleanUtils.byteAsBoolean(original.get(i));
                        Boolean computedBoolean = BooleanUtils.byteAsBoolean(computed.get(offset + i));
                        Assert.nullSafeEquals(origBoolean, "origBoolean", computedBoolean, "computedBoolean");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> {
                        Boolean origBoolean = BooleanUtils.byteAsBoolean(original.get((int) key));
                        Boolean computedBoolean =
                                BooleanUtils.byteAsBoolean(computed.get(offset + off.getAndIncrement()));
                        Assert.nullSafeEquals(origBoolean, "origBoolean", computedBoolean, "computedBoolean");
                    });
                }
            });
        }
    }

    public void testByteChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, byte.class, (utO) -> {
                final WritableByteChunk<Values> chunk = utO.asWritableByteChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_BYTE : (byte) random.nextInt());
                }
            }, (utO, utC, subset, offset) -> {
                final WritableByteChunk<Values> original = utO.asWritableByteChunk();
                final WritableByteChunk<Values> computed = utC.asWritableByteChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testShortChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, short.class, (utO) -> {
                final WritableShortChunk<Values> chunk = utO.asWritableShortChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_SHORT : (short) random.nextInt());
                }
            }, (utO, utC, subset, offset) -> {
                final WritableShortChunk<Values> original = utO.asWritableShortChunk();
                final WritableShortChunk<Values> computed = utC.asWritableShortChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testIntChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, int.class, (utO) -> {
                final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : random.nextInt());
                }
            }, (utO, utC, subset, offset) -> {
                final WritableIntChunk<Values> original = utO.asWritableIntChunk();
                final WritableIntChunk<Values> computed = utC.asWritableIntChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testLongChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, long.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testFloatChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, float.class, (utO) -> {
                final WritableFloatChunk<Values> chunk = utO.asWritableFloatChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_FLOAT : random.nextFloat());
                }
            }, (utO, utC, subset, offset) -> {
                final WritableFloatChunk<Values> original = utO.asWritableFloatChunk();
                final WritableFloatChunk<Values> computed = utC.asWritableFloatChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testDoubleChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, double.class, (utO) -> {
                final WritableDoubleChunk<Values> chunk = utO.asWritableDoubleChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_DOUBLE : random.nextDouble());
                }
            }, (utO, utC, subset, offset) -> {
                final WritableDoubleChunk<Values> original = utO.asWritableDoubleChunk();
                final WritableDoubleChunk<Values> computed = utC.asWritableDoubleChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)",
                                computed.get(offset + i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllRowKeys(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(offset + off.getAndIncrement()),
                            "computed.get(offset + off.getAndIncrement())"));
                }
            });
        }
    }

    public void testInstantChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.NONE, opts, Instant.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testZDTAsLongChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.ZDT, opts, ZonedDateTime.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);

            testRoundTripSerialization(SpecialMode.ZDT_WITH_FACTOR, opts, ZonedDateTime.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    // convert S to NS
                    final long one_bil = 1_000_000_000L;
                    long val = Math.abs(random.nextLong()) % one_bil;
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : val * one_bil);
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testObjectSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, Object.class, initObjectChunk(Integer::toString),
                new ObjectIdentityValidator<>());
    }

    public void testStringSerializationDHNulls() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DH_NULLS, String.class,
                initObjectChunk(Integer::toString),
                new ObjectIdentityValidator<>());
    }

    public void testStringSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, Object.class, initObjectChunk(Integer::toString),
                new ObjectIdentityValidator<>());
    }

    public void testUniqueToStringSerializationDHNulls() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DH_NULLS, Object.class, initObjectChunk(Unique::new),
                new ObjectToStringValidator<>());
    }

    public void testUniqueToStringSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, Object.class, initObjectChunk(Unique::new),
                new ObjectToStringValidator<>());
    }

    public void testStringArrayDHNullsSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DH_NULLS, String[].class,
                BarrageColumnRoundTripTest::initStringArrayChunk, new ObjectIdentityValidator<>());
    }

    public void testStringArraySerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, String[].class,
                BarrageColumnRoundTripTest::initStringArrayChunk,
                new ObjectIdentityValidator<>());
    }

    public void testLongArraySerializationDHNulls() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DH_NULLS, long[].class,
                BarrageColumnRoundTripTest::initLongArrayChunk,
                new LongArrayIdentityValidator());
    }

    public void testLongArraySerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, long[].class,
                BarrageColumnRoundTripTest::initLongArrayChunk,
                new LongArrayIdentityValidator());
    }

    public void testLongVectorSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, LongVector.class,
                BarrageColumnRoundTripTest::initLongVectorChunk,
                new LongVectorIdentityValidator());
    }

    public void testLocalDateSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, LocalDate.class,
                BarrageColumnRoundTripTest::initLocalDateChunk,
                new LocalDateIdentityValidator());
    }

    public void testLocalTimeSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, LocalTime.class,
                BarrageColumnRoundTripTest::initLocalTimeChunk,
                new LocalTimeIdentityValidator());
    }

    public void testDurationSerialization() throws IOException {
        testRoundTripSerialization(SpecialMode.NONE, OPT_DEFAULT, Duration.class,
                BarrageColumnRoundTripTest::initDurationChunk,
                new DurationIdentityValidator());
    }

    private static class Unique {
        final int value;

        Unique(final int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }

        @Override
        public boolean equals(final Object obj) {
            if (!(obj instanceof Unique)) {
                return false;
            }
            final Unique other = (Unique) obj;
            return value == other.value;
        }
    }

    private static <T> Consumer<WritableChunk<Values>> initObjectChunk(final IntFunction<T> mapper) {
        return (untypedChunk) -> {
            final WritableObjectChunk<T, Values> chunk = untypedChunk.asWritableObjectChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, i % 7 == 0 ? null : mapper.apply(i));
            }
        };
    }

    private static void initStringArrayChunk(final WritableChunk<Values> untypedChunk) {
        final Random random = new Random(0);
        final WritableObjectChunk<String[], Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final boolean setNull = random.nextDouble() < 0.05;
            if (setNull) {
                chunk.set(i, null);
            } else {
                final int arrLen = random.nextInt(20);
                final String[] entry = new String[arrLen];
                for (int k = 0; k < arrLen; ++k) {
                    entry[k] = i + ":" + k;
                }
                chunk.set(i, entry);
            }
        }
    }

    private static void initLongArrayChunk(final WritableChunk<Values> untypedChunk) {
        final Random random = new Random(0);
        final WritableObjectChunk<long[], Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final boolean setNull = random.nextDouble() < 0.05;
            if (setNull) {
                chunk.set(i, null);
            } else {
                final int arrLen = random.nextInt(20);
                final long[] entry = new long[arrLen];
                for (int k = 0; k < arrLen; ++k) {
                    entry[k] = i * 10000L + k;
                }
                chunk.set(i, entry);
            }
        }
    }

    private static void initLongVectorChunk(final WritableChunk<Values> untypedChunk) {
        final Random random = new Random(0);
        final WritableObjectChunk<LongVector, Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final boolean setNull = random.nextDouble() < 0.05;
            if (setNull) {
                chunk.set(i, null);
            } else {
                final int arrLen = random.nextInt(20);
                final long[] entry = new long[arrLen];
                for (int k = 0; k < arrLen; ++k) {
                    entry[k] = i * 10000L + k;
                }
                chunk.set(i, new LongVectorDirect(entry));
            }
        }
    }

    private static void initLocalDateChunk(final WritableChunk<Values> untypedChunk) {
        final Random random = new Random(0);
        final WritableObjectChunk<LocalDate, Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final boolean setNull = random.nextDouble() < 0.05;
            if (setNull) {
                chunk.set(i, null);
            } else {
                chunk.set(i, LocalDate.ofEpochDay((i * 17L) % 365));
            }
        }
    }

    private static void initLocalTimeChunk(final WritableChunk<Values> untypedChunk) {
        final Random random = new Random(0);
        final WritableObjectChunk<LocalTime, Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final boolean setNull = random.nextDouble() < 0.05;
            if (setNull) {
                chunk.set(i, null);
            } else {
                chunk.set(i, LocalTime.ofNanoOfDay(i * 1700000L));
            }
        }
    }

    private static void initDurationChunk(final WritableChunk<Values> untypedChunk) {
        final Random random = new Random(0);
        final WritableObjectChunk<Duration, Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final boolean setNull = random.nextDouble() < 0.05;
            if (setNull) {
                chunk.set(i, null);
            } else {
                chunk.set(i, Duration.ofSeconds(i * 3600L, i * 123456789L % 1_000_000_000));
            }
        }
    }

    // ---- Run-End Encoded (REE) helpers ----

    /**
     * Unified validator for all primitive chunk types: uses ChunkEquals for element-by-element comparison.
     */
    private static void primitiveIdentityValidate(final WritableChunk<Values> utO, final WritableChunk<Values> utC,
            final RowSequence subset, final int offset) {
        final ChunkEquals eq = ChunkEquals.makeEqual(utO.getChunkType());
        if (subset == null) {
            Assert.eqTrue(eq.equalReduce(utO, utC.slice(offset, utO.size())), "chunks equal");
        } else {
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(key -> Assert.eqTrue(
                    eq.equalReduce(utO.slice((int) key, 1), utC.slice(offset + off.getAndIncrement(), 1)),
                    "element equal at key=" + key));
        }
    }

    // ---- REE test methods ----

    public void testRunEndEncodedIntSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            // General: mixed values with null sentinels
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, int.class, (utO) -> {
                final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : random.nextInt());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }

        // All-same: one run of the same value
        testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, OPT_DEFAULT, int.class, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, 42);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);

        // All-distinct: every row is a different value → N runs
        testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, OPT_DEFAULT, int.class, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, i);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);

        // All-null: one null run
        testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, OPT_DEFAULT, int.class, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, QueryConstants.NULL_INT);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);

        // Alternating value/null
        testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, OPT_DEFAULT, int.class, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, (i % 2 == 0) ? 999 : QueryConstants.NULL_INT);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
    }

    public void testRunEndEncodedLongSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, long.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedShortSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, short.class, (utO) -> {
                final WritableShortChunk<Values> chunk = utO.asWritableShortChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_SHORT : (short) random.nextInt());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedByteSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, byte.class, (utO) -> {
                final WritableByteChunk<Values> chunk = utO.asWritableByteChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_BYTE : (byte) random.nextInt());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedCharSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, char.class, (utO) -> {
                final WritableCharChunk<Values> chunk = utO.asWritableCharChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_CHAR : (char) random.nextInt());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedFloatSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, float.class, (utO) -> {
                final WritableFloatChunk<Values> chunk = utO.asWritableFloatChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_FLOAT : random.nextFloat());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedDoubleSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, double.class, (utO) -> {
                final WritableDoubleChunk<Values> chunk = utO.asWritableDoubleChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_DOUBLE : random.nextDouble());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    /** REE float round-trip with NaN, +Inf, and -Inf values. FloatComparisons.eq treats NaN == NaN. */
    public void testRunEndEncodedFloatSpecialValues() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, float.class, (utO) -> {
                final WritableFloatChunk<Values> chunk = utO.asWritableFloatChunk();
                // 8 rows, 6 runs: [NaN,NaN], [+Inf], [-Inf], [1.0f], [NULL_FLOAT], [0.0f,0.0f]
                // NaN==NaN is true in FloatComparisons.eq, so the two NaN rows collapse to one run.
                chunk.set(0, Float.NaN);
                chunk.set(1, Float.NaN);
                chunk.set(2, Float.POSITIVE_INFINITY);
                chunk.set(3, Float.NEGATIVE_INFINITY);
                chunk.set(4, 1.0f);
                chunk.set(5, QueryConstants.NULL_FLOAT);
                chunk.set(6, 0.0f);
                chunk.set(7, 0.0f);
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    /** REE double round-trip with NaN, +Inf, and -Inf values. DoubleComparisons.eq treats NaN == NaN. */
    public void testRunEndEncodedDoubleSpecialValues() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, double.class, (utO) -> {
                final WritableDoubleChunk<Values> chunk = utO.asWritableDoubleChunk();
                // 8 rows, 6 runs: [NaN,NaN], [+Inf], [-Inf], [1.0], [NULL_DOUBLE], [0.0,0.0]
                chunk.set(0, Double.NaN);
                chunk.set(1, Double.NaN);
                chunk.set(2, Double.POSITIVE_INFINITY);
                chunk.set(3, Double.NEGATIVE_INFINITY);
                chunk.set(4, 1.0);
                chunk.set(5, QueryConstants.NULL_DOUBLE);
                chunk.set(6, 0.0);
                chunk.set(7, 0.0);
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedBooleanSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, boolean.class, (utO) -> {
                final WritableByteChunk<Values> chunk = utO.asWritableByteChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, BooleanUtils.booleanAsByte(i % 7 == 0 ? null : random.nextBoolean()));
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedStringSerialization() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, String.class,
                    initObjectChunk(Integer::toString),
                    new ObjectIdentityValidator<>());
        }
    }

    public void testRunEndEncodedInstantSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, Instant.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    public void testRunEndEncodedLocalDateSerialization() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, LocalDate.class,
                    BarrageColumnRoundTripTest::initLocalDateChunk,
                    new LocalDateIdentityValidator());
        }
    }

    public void testRunEndEncodedLocalTimeSerialization() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, LocalTime.class,
                    BarrageColumnRoundTripTest::initLocalTimeChunk,
                    new LocalTimeIdentityValidator());
        }
    }

    public void testRunEndEncodedDurationSerialization() throws IOException {
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED, opts, Duration.class,
                    BarrageColumnRoundTripTest::initDurationChunk,
                    new DurationIdentityValidator());
        }
    }

    /** Test that REE works correctly when the run_ends child uses Int16 (16-bit) indexing. */
    public void testRunEndEncodedInt16RunEndsSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED_INT16, opts, int.class, (utO) -> {
                final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : random.nextInt());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    /** Test that REE works correctly when the run_ends child uses Int64 (64-bit) indexing. */
    public void testRunEndEncodedInt64RunEndsSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : OPTIONS) {
            testRoundTripSerialization(SpecialMode.RUN_END_ENCODED_INT64, opts, int.class, (utO) -> {
                final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : random.nextInt());
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
    }

    /** Int16 run_ends with N > Short.MAX_VALUE must throw INVALID_ARGUMENT. */
    public void testRunEndEncodedOverflowGuardThrows() {
        try {
            RunEndEncodedChunkWriter.checkRunEndsOverflow(Short.MAX_VALUE + 1, ChunkType.Short);
            fail("Expected StatusRuntimeException for Int16 run_ends overflow");
        } catch (final StatusRuntimeException e) {
            assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
        }
        // Boundary and wider types should not throw.
        RunEndEncodedChunkWriter.checkRunEndsOverflow(Short.MAX_VALUE, ChunkType.Short);
        RunEndEncodedChunkWriter.checkRunEndsOverflow(Integer.MAX_VALUE, ChunkType.Int);
        RunEndEncodedChunkWriter.checkRunEndsOverflow(Integer.MAX_VALUE, ChunkType.Long);
    }

    /**
     * Verifies two internal paths in RunEndEncodedChunkInputStream that are not exercised by the main round-trip
     * harness:
     * <ul>
     * <li>getRawSize() cache-hit branch (second call to available() returns cached value)</li>
     * <li>drainTo() hasBeenRead guard (second call returns 0 without re-draining)</li>
     * </ul>
     */
    public void testRunEndEncodedDrainToIdempotentAndRawSizeCache() throws IOException {
        // Build a minimal REE int field with Int32 run_ends.
        final ByteString stdSchemaBytes = BarrageUtil.schemaBytesFromTableDefinition(
                TableDefinition.of(ColumnDefinition.of("col", Type.find(int.class))),
                Collections.emptyMap(), false);
        final Schema stdSchema = SchemaHelper.flatbufSchema(stdSchemaBytes.asReadOnlyByteBuffer());
        final org.apache.arrow.vector.types.pojo.Field stdPojoField =
                org.apache.arrow.vector.types.pojo.Field.convertField(stdSchema.fields(0));
        final org.apache.arrow.vector.types.pojo.Field runEndsField =
                new org.apache.arrow.vector.types.pojo.Field("run_ends",
                        new FieldType(false, new ArrowType.Int(32, true), null, null), Collections.emptyList());
        final org.apache.arrow.vector.types.pojo.Field valuesField =
                new org.apache.arrow.vector.types.pojo.Field("values",
                        stdPojoField.getFieldType(), stdPojoField.getChildren());
        final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                        new org.apache.arrow.vector.types.pojo.Field("col",
                                new FieldType(true, ArrowType.RunEndEncoded.INSTANCE, null, null),
                                List.of(runEndsField, valuesField))));
        final Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(pojoSchema.serializeAsMessage()));
        final org.apache.arrow.flatbuf.Field writerField = schema.fields(0);

        // context takes ownership of data and closes it — do not wrap data in try-with-resources.
        final WritableIntChunk<Values> data = WritableIntChunk.makeWritableChunk(4);
        data.set(0, 1);
        data.set(1, 1);
        data.set(2, 2);
        data.set(3, 2);
        data.setSize(4);
        final ChunkWriter<Chunk<Values>> writer = DefaultChunkWriterFactory.INSTANCE
                .newWriter(BarrageTypeInfo.make(int.class, null, writerField));
        try (final ChunkWriter.Context context = writer.makeContext(data, 0);
                final ChunkWriter.DrainableColumn column = writer.getInputStream(context, null, OPT_DEFAULT)) {
            // Two calls to available() — the second hits the cached-size branch.
            final int size1 = column.available();
            final int size2 = column.available();
            assertEquals(size1, size2);

            // Two calls to drainTo() — the second hits the hasBeenRead guard and returns 0.
            final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
            final int drained1 = column.drainTo(baos);
            assertTrue(drained1 > 0);
            final int drained2 = column.drainTo(baos);
            assertEquals(0, drained2);
        }
    }

    /**
     * REE-encoded wire size must be smaller than plain encoding when all rows share the same long value.
     *
     * <p>
     * With 1024 identical longs the standard encoding is ~8 KiB (validity + 8 bytes/row). The REE encoding collapses
     * everything to a single run: one Int32 run_end + one Int64 value ≈ 12 bytes.
     */
    public void testRunEndEncodedSizeReductionLong() throws IOException {
        final int numRows = 1024;

        // Build the standard Int64 field.
        final ByteString stdSchemaBytes = BarrageUtil.schemaBytesFromTableDefinition(
                TableDefinition.of(ColumnDefinition.of("col", Type.find(long.class))),
                Collections.emptyMap(), false);
        final Schema stdSchema = SchemaHelper.flatbufSchema(stdSchemaBytes.asReadOnlyByteBuffer());
        final Field stdField = stdSchema.fields(0);
        final org.apache.arrow.vector.types.pojo.Field stdPojoField =
                org.apache.arrow.vector.types.pojo.Field.convertField(stdField);

        // Build the REE field wrapping the same Int64 values child.
        final org.apache.arrow.vector.types.pojo.Field runEndsField =
                new org.apache.arrow.vector.types.pojo.Field("run_ends",
                        new FieldType(false, new ArrowType.Int(32, true), null, null),
                        Collections.emptyList());
        final org.apache.arrow.vector.types.pojo.Field valuesField =
                new org.apache.arrow.vector.types.pojo.Field("values",
                        stdPojoField.getFieldType(), stdPojoField.getChildren());
        final org.apache.arrow.vector.types.pojo.Schema reePojoSchema =
                new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                        new org.apache.arrow.vector.types.pojo.Field("col",
                                new FieldType(true, ArrowType.RunEndEncoded.INSTANCE, null, null),
                                List.of(runEndsField, valuesField))));
        final Schema reeSchema =
                SchemaHelper.flatbufSchema(ByteBuffer.wrap(reePojoSchema.serializeAsMessage()));
        final Field reeField = reeSchema.fields(0);

        // Standard writer: context takes ownership of the chunk.
        final WritableLongChunk<Values> stdData = WritableLongChunk.makeWritableChunk(numRows);
        for (int i = 0; i < numRows; i++) {
            stdData.set(i, 42L);
        }
        stdData.setSize(numRows);
        final ChunkWriter<Chunk<Values>> stdWriter =
                DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(long.class, null, stdField));
        final int stdSize;
        try (final ChunkWriter.Context ctx = stdWriter.makeContext(stdData, 0);
                final ChunkWriter.DrainableColumn col = stdWriter.getInputStream(ctx, null, OPT_DEFAULT)) {
            stdSize = col.available();
        }

        // REE writer: context takes ownership of the chunk.
        final WritableLongChunk<Values> reeData = WritableLongChunk.makeWritableChunk(numRows);
        for (int i = 0; i < numRows; i++) {
            reeData.set(i, 42L);
        }
        reeData.setSize(numRows);
        final ChunkWriter<Chunk<Values>> reeWriter =
                DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(long.class, null, reeField));
        final int reeSize;
        try (final ChunkWriter.Context ctx = reeWriter.makeContext(reeData, 0);
                final ChunkWriter.DrainableColumn col = reeWriter.getInputStream(ctx, null, OPT_DEFAULT)) {
            reeSize = col.available();
        }

        assertTrue("REE size " + reeSize + " should be smaller than standard size " + stdSize,
                reeSize < stdSize);

        final int reduction = stdSize - reeSize;
        final double pct = 100.0 * reduction / stdSize;
        System.out.printf("values=%d  original=%d  encoded=%d  reduction=%d (%.1f%%)%n",
                numRows, stdSize, reeSize, reduction, pct);
    }

    /**
     * REE-encoded wire size must be smaller than plain encoding when all rows share the same String value.
     *
     * <p>
     * With 1024 identical strings the standard encoding is dominated by offsets (1025 × 4 bytes) plus the repeated
     * payload. The REE encoding stores a single run: one Int32 run_end + one VarBinary entry.
     */
    public void testRunEndEncodedSizeReductionString() throws IOException {
        final int numRows = 1024;
        final String repeatedValue = "hello-world";

        // Build the standard Utf8 field.
        final ByteString stdSchemaBytes = BarrageUtil.schemaBytesFromTableDefinition(
                TableDefinition.of(ColumnDefinition.of("col", Type.find(String.class))),
                Collections.emptyMap(), false);
        final Schema stdSchema = SchemaHelper.flatbufSchema(stdSchemaBytes.asReadOnlyByteBuffer());
        final Field stdField = stdSchema.fields(0);
        final org.apache.arrow.vector.types.pojo.Field stdPojoField =
                org.apache.arrow.vector.types.pojo.Field.convertField(stdField);

        // Build the REE field wrapping the same Utf8 values child.
        final org.apache.arrow.vector.types.pojo.Field runEndsField =
                new org.apache.arrow.vector.types.pojo.Field("run_ends",
                        new FieldType(false, new ArrowType.Int(32, true), null, null),
                        Collections.emptyList());
        final org.apache.arrow.vector.types.pojo.Field valuesField =
                new org.apache.arrow.vector.types.pojo.Field("values",
                        stdPojoField.getFieldType(), stdPojoField.getChildren());
        final org.apache.arrow.vector.types.pojo.Schema reePojoSchema =
                new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                        new org.apache.arrow.vector.types.pojo.Field("col",
                                new FieldType(true, ArrowType.RunEndEncoded.INSTANCE, null, null),
                                List.of(runEndsField, valuesField))));
        final Schema reeSchema =
                SchemaHelper.flatbufSchema(ByteBuffer.wrap(reePojoSchema.serializeAsMessage()));
        final Field reeField = reeSchema.fields(0);

        // Standard writer: context takes ownership of the chunk.
        final WritableObjectChunk<String, Values> stdData = WritableObjectChunk.makeWritableChunk(numRows);
        for (int i = 0; i < numRows; i++) {
            stdData.set(i, repeatedValue);
        }
        stdData.setSize(numRows);
        final ChunkWriter<Chunk<Values>> stdWriter =
                DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(String.class, null, stdField));
        final int stdSize;
        try (final ChunkWriter.Context ctx = stdWriter.makeContext(stdData, 0);
                final ChunkWriter.DrainableColumn col = stdWriter.getInputStream(ctx, null, OPT_DEFAULT)) {
            stdSize = col.available();
        }

        // REE writer: context takes ownership of the chunk.
        final WritableObjectChunk<String, Values> reeData = WritableObjectChunk.makeWritableChunk(numRows);
        for (int i = 0; i < numRows; i++) {
            reeData.set(i, repeatedValue);
        }
        reeData.setSize(numRows);
        final ChunkWriter<Chunk<Values>> reeWriter =
                DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(String.class, null, reeField));
        final int reeSize;
        try (final ChunkWriter.Context ctx = reeWriter.makeContext(reeData, 0);
                final ChunkWriter.DrainableColumn col = reeWriter.getInputStream(ctx, null, OPT_DEFAULT)) {
            reeSize = col.available();
        }

        assertTrue("REE size " + reeSize + " should be smaller than standard size " + stdSize,
                reeSize < stdSize);

        final int reduction = stdSize - reeSize;
        final double pct = 100.0 * reduction / stdSize;
        System.out.printf("values=%d  original=%d  encoded=%d  reduction=%d (%.1f%%)%n",
                numRows, stdSize, reeSize, reduction, pct);
    }

    private interface Validator {
        void assertExpected(
                final WritableChunk<Values> original,
                final WritableChunk<Values> computed,
                @Nullable final RowSequence subset,
                final int offset);
    }

    private static final class ObjectIdentityValidator<T> implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> unTypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<T, Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<T, Values> computed = unTypedComputed.asWritableObjectChunk();

            if (subset == null) {
                subset = RowSetFactory.flat(untypedOriginal.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(key -> {
                final T next = original.get((int) key);
                if (next == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else if (next.getClass().isArray()) {
                    // noinspection unchecked
                    final T[] nt = (T[]) next;
                    // noinspection unchecked
                    final T[] ct = (T[]) computed.get(offset + off.getAndIncrement());
                    Assert.equals(nt.length, "nt.length", ct.length, "ct.length");
                    for (int k = 0; k < nt.length; ++k) {
                        Assert.equals(nt[k], "nt[k]", ct[k], "ct[k]");
                    }
                } else {
                    Assert.equals(next, "next", computed.get(offset + off.getAndIncrement()), "computed.get(i)");
                }
            });
        }
    }

    private static class ObjectToStringValidator<T> implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> untypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<T, Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<String, Values> computed = untypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = RowSetFactory.flat(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(key -> {
                if (original.get((int) key) == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else {
                    Assert.equals(original.get((int) key).toString(), "original.get(key).toString()",
                            computed.get(offset + off.getAndIncrement()), "computed.get(off.getAndIncrement())");
                }
            });
        }
    }

    private static final class LongArrayIdentityValidator implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> unTypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<long[], Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<long[], Values> computed = unTypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = RowSetFactory.flat(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(i -> {
                final long[] nt = original.get((int) i);
                if (nt == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else {
                    final long[] ct = computed.get(offset + off.getAndIncrement());
                    if (ct.length != nt.length) {
                        System.out.println("found");
                    }
                    Assert.equals(nt.length, "nt.length", ct.length, "ct.length");
                    for (int k = 0; k < nt.length; ++k) {
                        Assert.equals(nt[k], "nt[k]", ct[k], "ct[k]");
                    }
                }
            });
        }
    }

    private static final class LongVectorIdentityValidator implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> unTypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<LongVector, Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<LongVector, Values> computed = unTypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = RowSetFactory.flat(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(i -> {
                final LongVector ntv = original.get((int) i);
                if (ntv == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else {
                    final long[] nt = ntv.toArray();
                    final long[] ct = computed.get(offset + off.getAndIncrement()).toArray();
                    if (ct.length != nt.length) {
                        System.out.println("found");
                    }
                    Assert.equals(nt.length, "nt.length", ct.length, "ct.length");
                    for (int k = 0; k < nt.length; ++k) {
                        Assert.equals(nt[k], "nt[k]", ct[k], "ct[k]");
                    }
                }
            });
        }
    }

    private static final class LocalDateIdentityValidator implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> unTypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<LocalDate, Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<LocalDate, Values> computed = unTypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = RowSetFactory.flat(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(i -> {
                final LocalDate ld = original.get((int) i);
                if (ld == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else {
                    Assert.equals(ld, "ld", computed.get(offset + off.getAndIncrement()), "computed");
                }
            });
        }
    }

    private static final class LocalTimeIdentityValidator implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> unTypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<LocalTime, Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<LocalTime, Values> computed = unTypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = RowSetFactory.flat(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(i -> {
                final LocalTime lt = original.get((int) i);
                if (lt == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else {
                    Assert.equals(lt, "lt", computed.get(offset + off.getAndIncrement()), "computed");
                }
            });
        }
    }

    private static final class DurationIdentityValidator implements Validator {
        @Override
        public void assertExpected(
                final WritableChunk<Values> untypedOriginal,
                final WritableChunk<Values> unTypedComputed,
                @Nullable RowSequence subset,
                final int offset) {
            final WritableObjectChunk<Duration, Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<Duration, Values> computed = unTypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = RowSetFactory.flat(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllRowKeys(i -> {
                final Duration d = original.get((int) i);
                if (d == null) {
                    Assert.eqNull(computed.get(offset + off.getAndIncrement()), "computed");
                } else {
                    Assert.equals(d, "d", computed.get(offset + off.getAndIncrement()), "computed");
                }
            });
        }
    }

    // ---- Dictionary-encoded test methods ----

    public void testDictionaryEncodedIntSerialization() throws IOException {
        // Int32 index (default)
        for (final BarrageSubscriptionOptions opts : new BarrageSubscriptionOptions[] {OPT_DEFAULT, OPT_DH_NULLS}) {
            testDictionaryRoundTrip(32, int.class, opts, (utO) -> {
                final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : i % 5);
                }
            }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        }
        // Int8 index
        testDictionaryRoundTrip(8, int.class, OPT_DEFAULT, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : i % 3);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        // Int16 index
        testDictionaryRoundTrip(16, int.class, OPT_DEFAULT, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : i % 3);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
        // Int64 index
        testDictionaryRoundTrip(64, int.class, OPT_DEFAULT, (utO) -> {
            final WritableIntChunk<Values> chunk = utO.asWritableIntChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : i % 4);
            }
        }, BarrageColumnRoundTripTest::primitiveIdentityValidate);
    }

    public void testDictionaryEncodedInt8OverflowThrows() throws IOException {
        // 129 distinct int values exceed the Int8 dictionary limit of 128 (indices 0..127)
        final int OVERFLOW_SIZE = 129;
        final Field writerField = buildDictionaryField(int.class, 8, 0L);
        final DictionaryChunkWriter dictWriter = (DictionaryChunkWriter) DefaultChunkWriterFactory.INSTANCE.newWriter(
                BarrageTypeInfo.make(int.class, null, writerField));
        try (final WritableIntChunk<Values> srcData = WritableIntChunk.makeWritableChunk(OVERFLOW_SIZE)) {
            srcData.setSize(OVERFLOW_SIZE);
            for (int i = 0; i < OVERFLOW_SIZE; ++i) {
                srcData.set(i, i); // 0..128, all distinct
            }
            // makeContext takes ownership of the work chunk; pass a copy so srcData remains valid
            final WritableIntChunk<Values> work = WritableIntChunk.makeWritableChunk(OVERFLOW_SIZE);
            work.copyFromChunk(srcData, 0, 0, OVERFLOW_SIZE);
            final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
            try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0)) {
                try {
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx, null, OPT_DEFAULT, state)) {
                        col.drainTo(new ExposedByteArrayOutputStream());
                    }
                    fail("Expected IllegalStateException when dictionary exceeds Int8 limit");
                } catch (final IllegalStateException e) {
                    assertTrue("message should mention 'Int8': " + e.getMessage(),
                            e.getMessage().contains("Int8"));
                    assertTrue("message should mention '128': " + e.getMessage(),
                            e.getMessage().contains("128"));
                }
            }
        }
    }

    public void testDictionaryEncodedInt16OverflowThrows() throws IOException {
        // 32769 distinct int values exceed the Int16 dictionary limit of 32768 (indices 0..32767)
        final int OVERFLOW_SIZE = 32769;
        final Field writerField = buildDictionaryField(int.class, 16, 0L);
        final DictionaryChunkWriter dictWriter = (DictionaryChunkWriter) DefaultChunkWriterFactory.INSTANCE.newWriter(
                BarrageTypeInfo.make(int.class, null, writerField));
        try (final WritableIntChunk<Values> srcData = WritableIntChunk.makeWritableChunk(OVERFLOW_SIZE)) {
            srcData.setSize(OVERFLOW_SIZE);
            for (int i = 0; i < OVERFLOW_SIZE; ++i) {
                srcData.set(i, i); // 0..32768, all distinct
            }
            // makeContext takes ownership of the work chunk; pass a copy so srcData remains valid
            final WritableIntChunk<Values> work = WritableIntChunk.makeWritableChunk(OVERFLOW_SIZE);
            work.copyFromChunk(srcData, 0, 0, OVERFLOW_SIZE);
            final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
            try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0)) {
                try {
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx, null, OPT_DEFAULT, state)) {
                        col.drainTo(new ExposedByteArrayOutputStream());
                    }
                    fail("Expected IllegalStateException when dictionary exceeds Int16 limit");
                } catch (final IllegalStateException e) {
                    assertTrue("message should mention 'Int16': " + e.getMessage(),
                            e.getMessage().contains("Int16"));
                    assertTrue("message should mention '32768': " + e.getMessage(),
                            e.getMessage().contains("32768"));
                }
            }
        }
    }

    public void testDictionaryEncodedStringSerialization() throws IOException {
        final String[] words = {"cat", "dog", "fish"};
        for (final BarrageSubscriptionOptions opts : new BarrageSubscriptionOptions[] {OPT_DEFAULT, OPT_DH_NULLS}) {
            testDictionaryRoundTrip(32, String.class, opts, (utO) -> {
                @SuppressWarnings("unchecked")
                final WritableObjectChunk<Object, Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 5 == 0 ? null : words[i % words.length]);
                }
            }, new ObjectIdentityValidator<>());
        }
    }

    /** Two batches: second batch introduces a new dictionary value (delta append). */
    public void testDictionaryEncodedMultiBatchDelta() throws IOException {
        final int NUM_ROWS = 5;
        final Field writerField = buildDictionaryField(String.class, 32, 0L);
        final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
        final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
        final DictionaryChunkWriter dictWriter = (DictionaryChunkWriter) DefaultChunkWriterFactory.INSTANCE.newWriter(
                BarrageTypeInfo.make(String.class, null, writerField));

        // Batch 1: cat/dog/fish/null/cat
        final WritableObjectChunk<Object, Values> b1Src = WritableObjectChunk.makeWritableChunk(NUM_ROWS);
        try (SafeCloseable ignored1 = b1Src) {
            b1Src.set(0, "cat");
            b1Src.set(1, "dog");
            b1Src.set(2, "fish");
            b1Src.set(3, null);
            b1Src.set(4, "cat");
            b1Src.setSize(NUM_ROWS);

            // Batch 2: cat/bird/null/dog/bird (bird is new — requires a delta)
            final WritableObjectChunk<Object, Values> b2Src = WritableObjectChunk.makeWritableChunk(NUM_ROWS);
            try (SafeCloseable ignored2 = b2Src) {
                b2Src.set(0, "cat");
                b2Src.set(1, "bird");
                b2Src.set(2, null);
                b2Src.set(3, "dog");
                b2Src.set(4, "bird");
                b2Src.setSize(NUM_ROWS);

                final byte[] b1Bytes;
                final long[] b1Buffers;
                final List<ChunkWriter.FieldNodeInfo> b1Nodes = new ArrayList<>();
                final byte[] b2Bytes;
                final long[] b2Buffers;
                final List<ChunkWriter.FieldNodeInfo> b2Nodes = new ArrayList<>();

                // Write batch 1.
                final WritableChunk<Values> b1Work = ChunkType.Object.makeWritableChunk(NUM_ROWS);
                b1Work.copyFromChunk(b1Src, 0, 0, NUM_ROWS);
                try (final ChunkWriter.Context ctx1 = dictWriter.makeContext(b1Work, 0);
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                    final LongStream.Builder bufBld = LongStream.builder();
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx1, null, OPT_DEFAULT, state)) {
                        col.visitFieldNodes((n, nc) -> b1Nodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                        col.visitBuffers(bufBld::add);
                        col.drainTo(baos);
                    }
                    b1Buffers = bufBld.build().toArray();
                    b1Bytes = Arrays.copyOf(baos.peekBuffer(), baos.size());
                }
                try (final WritableChunk<Values> deltaChunk = DictionaryChunkWriter.buildDeltaValuesChunk(
                        dictWriter.getValuesChunkType(), state.getDeltaValues())) {
                    registry.update(0L, deltaChunk, false); // first batch: isDelta=false
                }
                state.resetDelta();

                // Write batch 2.
                final WritableChunk<Values> b2Work = ChunkType.Object.makeWritableChunk(NUM_ROWS);
                b2Work.copyFromChunk(b2Src, 0, 0, NUM_ROWS);
                try (final ChunkWriter.Context ctx2 = dictWriter.makeContext(b2Work, 0);
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                    final LongStream.Builder bufBld = LongStream.builder();
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx2, null, OPT_DEFAULT, state)) {
                        col.visitFieldNodes((n, nc) -> b2Nodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                        col.visitBuffers(bufBld::add);
                        col.drainTo(baos);
                    }
                    b2Buffers = bufBld.build().toArray();
                    b2Bytes = Arrays.copyOf(baos.peekBuffer(), baos.size());
                }
                try (final WritableChunk<Values> deltaChunk = DictionaryChunkWriter.buildDeltaValuesChunk(
                        dictWriter.getValuesChunkType(), state.getDeltaValues())) {
                    registry.update(0L, deltaChunk, true); // second batch: isDelta=true (delta append)
                }
                state.resetDelta();

                // Registry now holds id=0 → [cat, dog, fish, bird]. Decode both batches and validate.
                @SuppressWarnings("unchecked")
                final ChunkReader<WritableChunk<Values>> reader =
                        (ChunkReader<WritableChunk<Values>>) (ChunkReader<?>) DefaultChunkReaderFactory.INSTANCE
                                .newReader(
                                        BarrageTypeInfo.make(String.class, null, writerField),
                                        OPT_DEFAULT, registry, null);

                try (final WritableChunk<Values> rt1 = reader.readChunk(
                        b1Nodes.iterator(), Arrays.stream(b1Buffers).iterator(),
                        new LittleEndianDataInputStream(new ByteArrayInputStream(b1Bytes)),
                        null, 0, NUM_ROWS)) {
                    new ObjectIdentityValidator<>().assertExpected(b1Src, rt1, null, 0);
                }
                try (final WritableChunk<Values> rt2 = reader.readChunk(
                        b2Nodes.iterator(), Arrays.stream(b2Buffers).iterator(),
                        new LittleEndianDataInputStream(new ByteArrayInputStream(b2Bytes)),
                        null, 0, NUM_ROWS)) {
                    new ObjectIdentityValidator<>().assertExpected(b2Src, rt2, null, 0);
                }

            } // end try(b2Src)
        } // end try(b1Src)
    }

    /**
     * Verifies that the Arrow standard path (useDeephavenNulls = false) emits a validity bitmap on the index column
     * when null rows are present, and omits it when there are none.
     */
    public void testDictionaryEncodedIndexValidityBitmap() throws IOException {
        final int NUM_ROWS = 8;
        final Field writerField = buildDictionaryField(int.class, 32, 0L);
        final DictionaryChunkWriter dictWriter = (DictionaryChunkWriter) DefaultChunkWriterFactory.INSTANCE.newWriter(
                BarrageTypeInfo.make(int.class, null, writerField));

        // --- case 1: column contains nulls — validity buffer must be present ---
        {
            // rows: 0=NULL, 1=1, 2=2, 3=NULL, 4=1, 5=3, 6=NULL, 7=2 => 3 nulls
            final int expectedNullCount = 3;
            try (final WritableIntChunk<Values> src = WritableIntChunk.makeWritableChunk(NUM_ROWS)) {
                src.setSize(NUM_ROWS);
                src.set(0, QueryConstants.NULL_INT);
                src.set(1, 1);
                src.set(2, 2);
                src.set(3, QueryConstants.NULL_INT);
                src.set(4, 1);
                src.set(5, 3);
                src.set(6, QueryConstants.NULL_INT);
                src.set(7, 2);

                final WritableIntChunk<Values> work = WritableIntChunk.makeWritableChunk(NUM_ROWS);
                work.copyFromChunk(src, 0, 0, NUM_ROWS);
                final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
                try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0);
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                    final LongStream.Builder bufBld = LongStream.builder();
                    final List<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx, null, OPT_DEFAULT, state)) {
                        col.visitFieldNodes((n, nc) -> fieldNodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                        col.visitBuffers(bufBld::add);
                        col.drainTo(baos);
                    }
                    final long[] buffers = bufBld.build().toArray();

                    Assert.eq(expectedNullCount, "expectedNullCount",
                            fieldNodes.get(0).nullCount, "fieldNodes.get(0).nullCount");
                    assertTrue("validity buffer must be present when nulls exist", buffers[0] > 0);
                }
            }
        }

        // --- case 2: no nulls — validity buffer must be absent (Arrow spec: optional when nullCount == 0) ---
        {
            try (final WritableIntChunk<Values> src = WritableIntChunk.makeWritableChunk(NUM_ROWS)) {
                src.setSize(NUM_ROWS);
                for (int i = 0; i < NUM_ROWS; ++i) {
                    src.set(i, i % 3);
                }

                final WritableIntChunk<Values> work = WritableIntChunk.makeWritableChunk(NUM_ROWS);
                work.copyFromChunk(src, 0, 0, NUM_ROWS);
                final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
                try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0);
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                    final LongStream.Builder bufBld = LongStream.builder();
                    final List<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx, null, OPT_DEFAULT, state)) {
                        col.visitFieldNodes((n, nc) -> fieldNodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                        col.visitBuffers(bufBld::add);
                        col.drainTo(baos);
                    }
                    final long[] buffers = bufBld.build().toArray();

                    Assert.eq(0, "zero", fieldNodes.get(0).nullCount, "fieldNodes.get(0).nullCount");
                    assertTrue("validity buffer must be absent when no nulls exist", buffers[0] == 0);
                }
            }
        }
    }

    private enum SpecialMode {
        NONE, MAP, VAR_LEN_LIST, FIXED_LEN_LIST, ZDT, ZDT_WITH_FACTOR, SPARSE_UNION, DENSE_UNION, RUN_END_ENCODED, RUN_END_ENCODED_INT16, RUN_END_ENCODED_INT64, DICTIONARY_ENCODED, DICTIONARY_ENCODED_INT8, DICTIONARY_ENCODED_INT16, DICTIONARY_ENCODED_INT64
    }

    private static <T> void testRoundTripSerialization(
            final SpecialMode mode,
            final BarrageSubscriptionOptions options,
            Class<T> type,
            final Consumer<WritableChunk<Values>> initData,
            final Validator validator) throws IOException {
        final int NUM_ROWS = 8;
        final ChunkType chunkType;
        final Class<T> readType;
        if (type == ZonedDateTime.class) {
            chunkType = ChunkType.Long;
            // noinspection unchecked
            readType = (Class<T>) long.class;
        } else {
            // noinspection unchecked
            type = (Class<T>) ReinterpretUtils.maybeConvertToPrimitiveDataType(type);
            if (type == Boolean.class || type == boolean.class) {
                chunkType = ChunkType.Byte;
            } else {
                chunkType = ChunkType.fromElementType(type);
            }

            if (type == Object.class) {
                // noinspection unchecked
                readType = (Class<T>) String.class;
            } else {
                readType = type;
            }
        }

        final Field writerField;
        if (mode == SpecialMode.MAP) {
            final Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put(DH_TYPE_TAG, Map.class.getCanonicalName());
            final FieldType fieldType = new FieldType(true, new ArrowType.Map(false), null, attributes);

            final List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
            children.add(new org.apache.arrow.vector.types.pojo.Field("key",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), null));
            children.add(new org.apache.arrow.vector.types.pojo.Field("value",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), null));
            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col", fieldType, List.of(
                                    new org.apache.arrow.vector.types.pojo.Field("struct",
                                            new FieldType(false, ArrowType.Struct.INSTANCE, null, null), children)))));

            byte[] schemaBytes = pojoSchema.serializeAsMessage();
            Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            writerField = schema.fields(0);
        } else if (mode == SpecialMode.VAR_LEN_LIST) {
            final Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put(DH_TYPE_TAG, String[].class.getCanonicalName());
            attributes.put(DH_COMPONENT_TYPE_TAG, String.class.getCanonicalName());

            final List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
            children.add(new org.apache.arrow.vector.types.pojo.Field("element",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), null));

            final FieldType fieldType = new FieldType(true, new ArrowType.List(), null, attributes);
            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col", fieldType, children)));

            byte[] schemaBytes = pojoSchema.serializeAsMessage();
            Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            writerField = schema.fields(0);
        } else if (mode == SpecialMode.FIXED_LEN_LIST) {
            final Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put(DH_TYPE_TAG, String[].class.getCanonicalName());
            attributes.put(DH_COMPONENT_TYPE_TAG, String.class.getCanonicalName());

            final List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
            children.add(new org.apache.arrow.vector.types.pojo.Field("element",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), null));

            final FieldType fieldType =
                    new FieldType(true, new ArrowType.FixedSizeList(FIXED_LIST_LEN), null, attributes);
            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col", fieldType, children)));

            byte[] schemaBytes = pojoSchema.serializeAsMessage();
            Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            writerField = schema.fields(0);
        } else if (mode == SpecialMode.ZDT_WITH_FACTOR || mode == SpecialMode.ZDT) {
            final Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put(DH_TYPE_TAG, ZonedDateTime.class.getCanonicalName());

            final TimeUnit tu = (mode == SpecialMode.ZDT_WITH_FACTOR) ? TimeUnit.SECOND : TimeUnit.NANOSECOND;
            final FieldType fieldType = new FieldType(
                    true, new ArrowType.Timestamp(tu, "America/New_York"), null, attributes);
            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col", fieldType, List.of())));

            byte[] schemaBytes = pojoSchema.serializeAsMessage();
            Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            writerField = schema.fields(0);
        } else if (mode == SpecialMode.DENSE_UNION || mode == SpecialMode.SPARSE_UNION) {
            final Map<String, String> attributes = new LinkedHashMap<>();
            attributes.put(DH_TYPE_TAG, Object.class.getCanonicalName());

            final int[] typeIds = new int[] {0, 1};
            final UnionMode unionMode = (mode == SpecialMode.DENSE_UNION)
                    ? UnionMode.Dense
                    : UnionMode.Sparse;
            final FieldType fieldType =
                    new FieldType(true, new ArrowType.Union(unionMode, typeIds), null, attributes);

            final List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
            children.add(new org.apache.arrow.vector.types.pojo.Field("longs",
                    new FieldType(true, new ArrowType.Int(64, true), null, null), null));
            children.add(new org.apache.arrow.vector.types.pojo.Field("strings",
                    new FieldType(true, ArrowType.Utf8.INSTANCE, null, null), null));

            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col", fieldType, children)));

            byte[] schemaBytes = pojoSchema.serializeAsMessage();
            Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            writerField = schema.fields(0);
        } else if (mode == SpecialMode.RUN_END_ENCODED
                || mode == SpecialMode.RUN_END_ENCODED_INT16
                || mode == SpecialMode.RUN_END_ENCODED_INT64) {
            // Build the REE parent field whose values child is the standard field for readType.
            final ByteString stdSchemaBytes = BarrageUtil.schemaBytesFromTableDefinition(
                    TableDefinition.of(ColumnDefinition.of("col", Type.find(readType))),
                    Collections.emptyMap(), false);
            final Schema stdSchema = SchemaHelper.flatbufSchema(stdSchemaBytes.asReadOnlyByteBuffer());
            final org.apache.arrow.vector.types.pojo.Field stdPojoField =
                    org.apache.arrow.vector.types.pojo.Field.convertField(stdSchema.fields(0));

            // run_ends child: non-nullable integer index
            final int runEndsWidth;
            if (mode == SpecialMode.RUN_END_ENCODED_INT16) {
                runEndsWidth = 16;
            } else if (mode == SpecialMode.RUN_END_ENCODED_INT64) {
                runEndsWidth = 64;
            } else {
                runEndsWidth = 32;
            }
            final org.apache.arrow.vector.types.pojo.Field runEndsField =
                    new org.apache.arrow.vector.types.pojo.Field("run_ends",
                            new FieldType(false, new ArrowType.Int(runEndsWidth, true), null, null),
                            Collections.emptyList());
            // values child: same Arrow type and metadata as the standard field
            final org.apache.arrow.vector.types.pojo.Field valuesField =
                    new org.apache.arrow.vector.types.pojo.Field("values",
                            stdPojoField.getFieldType(), stdPojoField.getChildren());

            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col",
                                    new FieldType(true, ArrowType.RunEndEncoded.INSTANCE, null, null),
                                    List.of(runEndsField, valuesField))));
            final byte[] schemaBytes = pojoSchema.serializeAsMessage();
            final Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            writerField = schema.fields(0);
        } else {
            ByteString schemaBytes = BarrageUtil.schemaBytesFromTableDefinition(
                    TableDefinition.of(ColumnDefinition.of("col", Type.find(readType))), Collections.emptyMap(), false);
            Schema schema = SchemaHelper.flatbufSchema(schemaBytes.asReadOnlyByteBuffer());
            writerField = schema.fields(0);
        }

        final Field readerField;
        if (!options.columnsAsList()) {
            readerField = writerField;
        } else {
            // the reader needs to see this field as wrapped in a list to properly pick a chunk reader and decode

            final org.apache.arrow.vector.types.pojo.Field origFieldPojo =
                    org.apache.arrow.vector.types.pojo.Field.convertField(writerField);
            final List<org.apache.arrow.vector.types.pojo.Field> children = new ArrayList<>();
            children.add(origFieldPojo);

            final FieldType fieldType = new FieldType(
                    false, Types.MinorType.LIST.getType(), origFieldPojo.getDictionary(), origFieldPojo.getMetadata());
            final org.apache.arrow.vector.types.pojo.Schema pojoSchema =
                    new org.apache.arrow.vector.types.pojo.Schema(Collections.singletonList(
                            new org.apache.arrow.vector.types.pojo.Field("col", fieldType, children)));

            byte[] schemaBytes = pojoSchema.serializeAsMessage();
            Schema schema = SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes));
            readerField = schema.fields(0);
        }

        final WritableChunk<Values> srcData = chunkType.makeWritableChunk(NUM_ROWS);
        initData.accept(srcData);

        // The writer owns data; it is allowed to close it prematurely if the data needs to be converted to primitive
        final WritableChunk<Values> data = chunkType.makeWritableChunk(NUM_ROWS);
        data.copyFromChunk(srcData, 0, 0, srcData.size());

        final ChunkWriter<Chunk<Values>> writer = DefaultChunkWriterFactory.INSTANCE
                .newWriter(BarrageTypeInfo.make(type, type.getComponentType(), writerField));
        try (SafeCloseable ignored = srcData;
                final ChunkWriter.Context context = writer.makeContext(data, 0)) {
            // full sub logic
            try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                    final ChunkWriter.DrainableColumn column = writer.getInputStream(context, null, options)) {
                final int numRows = srcData.size();

                final ArrayList<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                final LongStream.Builder bufferNodes = LongStream.builder();
                if (options.columnsAsList()) {
                    // if we are sending columns as a list, we need to add the list buffers before each column
                    final SingleElementListHeaderWriter listHeader = new SingleElementListHeaderWriter(numRows);
                    listHeader.visitFieldNodes((numElements, nullCount) -> fieldNodes
                            .add(new ChunkWriter.FieldNodeInfo(numElements, nullCount)));
                    listHeader.visitBuffers(bufferNodes::add);

                    final int startSize = baos.size();
                    final int available = listHeader.available();
                    listHeader.drainTo(baos);
                    if (available != baos.size() - startSize) {
                        throw new IllegalStateException("available=" + available + ", baos.size()=" + baos.size());
                    }
                }

                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkWriter.FieldNodeInfo(numElements, nullCount)));
                column.visitBuffers(bufferNodes::add);
                final int startSize = baos.size();
                final int available = column.available();
                final long[] buffers = bufferNodes.build().toArray();
                column.drainTo(baos);
                if (available != baos.size() - startSize) {
                    throw new IllegalStateException("available=" + available + ", baos.size()=" + baos.size());
                }

                final ThrowingConsumer<WritableChunk<Values>, IOException> doValidate = (outChunk) -> {
                    final int origSize = outChunk == null ? 0 : outChunk.size();
                    final DataInput dis =
                            new LittleEndianDataInputStream(
                                    new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                    WritableChunk<Values> rtData = null;
                    try {
                        rtData = readChunk(options, readType, readType.getComponentType(),
                                readerField, fieldNodes.iterator(), Arrays.stream(buffers).iterator(), dis, outChunk,
                                origSize, numRows);
                        Assert.eq(srcData.size(), "srcData.size()", rtData.size() - origSize,
                                "rtData.size() - origSize");
                        validator.assertExpected(srcData, rtData, null, origSize);
                    } finally {
                        if (outChunk == null && rtData != null) {
                            rtData.close();
                        }
                    }
                };

                doValidate.accept(null);
                try (final WritableChunk<Values> rtData = chunkType.makeWritableChunk(2 * NUM_ROWS)) {
                    rtData.setSize(0);
                    doValidate.accept(rtData);
                    // append another copy to ensure we can append to existing chunks
                    doValidate.accept(rtData);
                }
            }

            // empty subset
            try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                    final ChunkWriter.DrainableColumn column =
                            writer.getInputStream(context, RowSetFactory.empty(), options)) {
                final int numRows = 0;

                final ArrayList<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                final LongStream.Builder bufferNodes = LongStream.builder();
                if (options.columnsAsList()) {
                    // if we are sending columns as a list, we need to add the list buffers before each column
                    final SingleElementListHeaderWriter listHeader = new SingleElementListHeaderWriter(numRows);
                    listHeader.visitFieldNodes((numElements, nullCount) -> fieldNodes
                            .add(new ChunkWriter.FieldNodeInfo(numElements, nullCount)));
                    listHeader.visitBuffers(bufferNodes::add);

                    final int startSize = baos.size();
                    final int available = listHeader.available();
                    listHeader.drainTo(baos);
                    if (available != baos.size() - startSize) {
                        throw new IllegalStateException("available=" + available + ", baos.size()=" + baos.size());
                    }
                }

                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkWriter.FieldNodeInfo(numElements, nullCount)));
                column.visitBuffers(bufferNodes::add);
                final long[] buffers = bufferNodes.build().toArray();
                column.drainTo(baos);

                final ThrowingConsumer<WritableChunk<Values>, IOException> doValidate = (outChunk) -> {
                    final int origSize = outChunk == null ? 0 : outChunk.size();
                    final DataInput dis =
                            new LittleEndianDataInputStream(
                                    new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                    WritableChunk<Values> rtData = null;
                    try {
                        rtData = readChunk(options, readType, readType.getComponentType(),
                                readerField, fieldNodes.iterator(), Arrays.stream(buffers).iterator(), dis, outChunk,
                                origSize, numRows);
                        Assert.eq(rtData.size(), "rtData.size()", 0);
                    } finally {
                        if (outChunk == null && rtData != null) {
                            rtData.close();
                        }
                    }
                };

                doValidate.accept(null);
                try (final WritableChunk<Values> rtData = chunkType.makeWritableChunk(2 * NUM_ROWS)) {
                    rtData.setSize(0);
                    doValidate.accept(rtData);
                    // append another copy to ensure we can append to existing chunks
                    doValidate.accept(rtData);
                }
            }

            // swiss-cheese subset
            final Random random = new Random(0);
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (int i = 0; i < srcData.size(); ++i) {
                if (random.nextBoolean()) {
                    builder.appendKey(i);
                }
            }
            try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                    final RowSet subset = builder.build();
                    final ChunkWriter.DrainableColumn column =
                            writer.getInputStream(context, subset, options)) {
                final int numRows = subset.intSize();

                final ArrayList<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                final LongStream.Builder bufferNodes = LongStream.builder();
                if (options.columnsAsList()) {
                    // if we are sending columns as a list, we need to add the list buffers before each column
                    final SingleElementListHeaderWriter listHeader = new SingleElementListHeaderWriter(numRows);
                    listHeader.visitFieldNodes((numElements, nullCount) -> fieldNodes
                            .add(new ChunkWriter.FieldNodeInfo(numElements, nullCount)));
                    listHeader.visitBuffers(bufferNodes::add);

                    final int startSize = baos.size();
                    final int available = listHeader.available();
                    listHeader.drainTo(baos);
                    if (available != baos.size() - startSize) {
                        throw new IllegalStateException("available=" + available + ", baos.size()=" + baos.size());
                    }
                }

                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkWriter.FieldNodeInfo(numElements, nullCount)));
                column.visitBuffers(bufferNodes::add);
                final long[] buffers = bufferNodes.build().toArray();
                column.drainTo(baos);

                final ThrowingConsumer<WritableChunk<Values>, IOException> doValidate = (outChunk) -> {
                    final int origSize = outChunk == null ? 0 : outChunk.size();
                    final DataInput dis =
                            new LittleEndianDataInputStream(
                                    new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                    WritableChunk<Values> rtData = null;
                    try {
                        rtData = readChunk(options, readType, readType.getComponentType(),
                                readerField, fieldNodes.iterator(), Arrays.stream(buffers).iterator(), dis, outChunk,
                                origSize, numRows);
                        Assert.eq(subset.intSize(), "subset.intSize()", rtData.size() - origSize, "rtData.size()");
                        validator.assertExpected(srcData, rtData, subset, 0);
                    } finally {
                        if (outChunk == null && rtData != null) {
                            rtData.close();
                        }
                    }
                };

                doValidate.accept(null);
                try (final WritableChunk<Values> rtData = chunkType.makeWritableChunk(2 * NUM_ROWS)) {
                    rtData.setSize(0);
                    doValidate.accept(rtData);
                    // append another copy to ensure we can append to existing chunks
                    doValidate.accept(rtData);
                }
            }
        }
    }

    // ---- Dictionary-encoded helpers ----

    /**
     * Builds a flatbuf Arrow {@link Field} whose value type matches {@code valueType} but carries a
     * {@link DictionaryEncoding} with the given index bit width and dictionary id.
     */
    private static Field buildDictionaryField(
            final Class<?> valueType,
            final int indexBitWidth,
            final long dictId) {
        final ByteString stdSchemaBytes = BarrageUtil.schemaBytesFromTableDefinition(
                TableDefinition.of(ColumnDefinition.of("col", Type.find(valueType))),
                Collections.emptyMap(), false);
        final Schema stdSchema =
                SchemaHelper.flatbufSchema(stdSchemaBytes.asReadOnlyByteBuffer());
        final org.apache.arrow.vector.types.pojo.Field stdPojoField =
                org.apache.arrow.vector.types.pojo.Field.convertField(stdSchema.fields(0));
        final DictionaryEncoding dictEncoding =
                new DictionaryEncoding(dictId, false, new ArrowType.Int(indexBitWidth, true));
        final org.apache.arrow.vector.types.pojo.Field dictPojoField =
                new org.apache.arrow.vector.types.pojo.Field(
                        stdPojoField.getName(),
                        new FieldType(stdPojoField.isNullable(), stdPojoField.getType(),
                                dictEncoding, stdPojoField.getMetadata()),
                        stdPojoField.getChildren());
        final byte[] schemaBytes = new org.apache.arrow.vector.types.pojo.Schema(
                Collections.singletonList(dictPojoField)).serializeAsMessage();
        return SchemaHelper.flatbufSchema(ByteBuffer.wrap(schemaBytes)).fields(0);
    }

    /**
     * Single-batch dictionary round-trip: writes {@code NUM_ROWS} rows using the dictionary writer, emits the delta to
     * a fresh reader registry, then reads back and validates against the original data for the full subset, the empty
     * subset, and a random swiss-cheese subset.
     */
    @SuppressWarnings("unchecked")
    private static <T> void testDictionaryRoundTrip(
            final int indexBitWidth,
            final Class<T> type,
            final BarrageSubscriptionOptions options,
            final Consumer<WritableChunk<Values>> initData,
            final Validator validator) throws IOException {
        final int NUM_ROWS = 8;
        final ChunkType chunkType = (type == Boolean.class || type == boolean.class)
                ? ChunkType.Byte
                : ChunkType.fromElementType(type);

        final Field writerField = buildDictionaryField(type, indexBitWidth, 0L);
        final DictionaryChunkWriter dictWriter = (DictionaryChunkWriter) DefaultChunkWriterFactory.INSTANCE.newWriter(
                BarrageTypeInfo.make(type, null, writerField));

        try (final WritableChunk<Values> srcData = chunkType.makeWritableChunk(NUM_ROWS)) {
            srcData.setSize(NUM_ROWS);
            initData.accept(srcData);

            // --- full batch ---
            {
                final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
                final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableChunk<Values> work = chunkType.makeWritableChunk(NUM_ROWS);
                work.copyFromChunk(srcData, 0, 0, NUM_ROWS);
                try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0);
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                    final LongStream.Builder bufBld = LongStream.builder();
                    final List<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx, null, options, state)) {
                        col.visitFieldNodes((n, nc) -> fieldNodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                        col.visitBuffers(bufBld::add);
                        col.drainTo(baos);
                    }
                    final long[] buffers = bufBld.build().toArray();
                    try (final WritableChunk<Values> deltaChunk = DictionaryChunkWriter.buildDeltaValuesChunk(
                            dictWriter.getValuesChunkType(), state.getDeltaValues())) {
                        registry.update(0L, deltaChunk, false);
                    }
                    state.resetDelta();
                    final ChunkReader<WritableChunk<Values>> reader =
                            (ChunkReader<WritableChunk<Values>>) (ChunkReader<?>) DefaultChunkReaderFactory.INSTANCE
                                    .newReader(
                                            BarrageTypeInfo.make(type, null, writerField), options, registry, null);
                    try (final WritableChunk<Values> rt = reader.readChunk(
                            fieldNodes.iterator(), Arrays.stream(buffers).iterator(),
                            new LittleEndianDataInputStream(
                                    new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size())),
                            null, 0, NUM_ROWS)) {
                        Assert.eq(NUM_ROWS, "NUM_ROWS", rt.size(), "rt.size()");
                        validator.assertExpected(srcData, rt, null, 0);
                    }
                }
            }

            // --- empty subset: reader must handle numRows=0 without consulting the registry ---
            {
                final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
                final WritableChunk<Values> work = chunkType.makeWritableChunk(NUM_ROWS);
                work.copyFromChunk(srcData, 0, 0, NUM_ROWS);
                try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0);
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                    final LongStream.Builder bufBld = LongStream.builder();
                    final List<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                    try (final ChunkWriter.DrainableColumn col =
                            dictWriter.getInputStream(ctx, RowSetFactory.empty(), options, state)) {
                        col.visitFieldNodes((n, nc) -> fieldNodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                        col.visitBuffers(bufBld::add);
                        col.drainTo(baos);
                    }
                    final long[] buffers = bufBld.build().toArray();
                    // Empty registry is valid because DictionaryChunkReader exits early for numRows=0.
                    final ChunkReader<WritableChunk<Values>> reader =
                            (ChunkReader<WritableChunk<Values>>) (ChunkReader<?>) DefaultChunkReaderFactory.INSTANCE
                                    .newReader(
                                            BarrageTypeInfo.make(type, null, writerField), options,
                                            new DictionaryReaderRegistry(), null);
                    try (final WritableChunk<Values> rt = reader.readChunk(
                            fieldNodes.iterator(), Arrays.stream(buffers).iterator(),
                            new LittleEndianDataInputStream(
                                    new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size())),
                            null, 0, 0)) {
                        Assert.eq(0, "zero", rt.size(), "rt.size()");
                    }
                }
            }

            // --- swiss-cheese subset ---
            {
                final DictionaryWriterState state = new LocalDictionaryWriterState(0L);
                final DictionaryReaderRegistry registry = new DictionaryReaderRegistry();
                final WritableChunk<Values> work = chunkType.makeWritableChunk(NUM_ROWS);
                work.copyFromChunk(srcData, 0, 0, NUM_ROWS);
                try (final ChunkWriter.Context ctx = dictWriter.makeContext(work, 0)) {
                    final Random random = new Random(1);
                    final RowSetBuilderSequential rowBld = RowSetFactory.builderSequential();
                    for (int i = 0; i < NUM_ROWS; ++i) {
                        if (random.nextBoolean()) {
                            rowBld.appendKey(i);
                        }
                    }
                    try (final RowSet subset = rowBld.build();
                            final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
                        final LongStream.Builder bufBld = LongStream.builder();
                        final List<ChunkWriter.FieldNodeInfo> fieldNodes = new ArrayList<>();
                        try (final ChunkWriter.DrainableColumn col =
                                dictWriter.getInputStream(ctx, subset, options, state)) {
                            col.visitFieldNodes((n, nc) -> fieldNodes.add(new ChunkWriter.FieldNodeInfo(n, nc)));
                            col.visitBuffers(bufBld::add);
                            col.drainTo(baos);
                        }
                        final long[] buffers = bufBld.build().toArray();
                        if (state.hasDelta()) {
                            try (final WritableChunk<Values> deltaChunk = DictionaryChunkWriter.buildDeltaValuesChunk(
                                    dictWriter.getValuesChunkType(), state.getDeltaValues())) {
                                registry.update(0L, deltaChunk, false);
                            }
                            state.resetDelta();
                        }
                        final int subsetSize = subset.intSize();
                        if (subsetSize == 0) {
                            return;
                        }
                        final ChunkReader<WritableChunk<Values>> reader =
                                (ChunkReader<WritableChunk<Values>>) (ChunkReader<?>) DefaultChunkReaderFactory.INSTANCE
                                        .newReader(
                                                BarrageTypeInfo.make(type, null, writerField), options, registry, null);
                        try (final WritableChunk<Values> rt = reader.readChunk(
                                fieldNodes.iterator(), Arrays.stream(buffers).iterator(),
                                new LittleEndianDataInputStream(
                                        new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size())),
                                null, 0, subsetSize)) {
                            Assert.eq(subsetSize, "subsetSize", rt.size(), "rt.size()");
                            validator.assertExpected(srcData, rt, subset, 0);
                        }
                    }
                }
            }

        } // end try(srcData)
    }
}
