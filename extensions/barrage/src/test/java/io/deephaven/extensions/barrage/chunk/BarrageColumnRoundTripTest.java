//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
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
            }, BarrageColumnRoundTripTest::longIdentityValidator);
        }
    }

    private static void longIdentityValidator(WritableChunk<Values> utO, WritableChunk<Values> utC, RowSequence subset,
            int offset) {
        final WritableLongChunk<Values> original = utO.asWritableLongChunk();
        final WritableLongChunk<Values> computed = utC.asWritableLongChunk();
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
            }, BarrageColumnRoundTripTest::longIdentityValidator);
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
            }, BarrageColumnRoundTripTest::longIdentityValidator);

            testRoundTripSerialization(SpecialMode.ZDT_WITH_FACTOR, opts, ZonedDateTime.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    // convert S to NS
                    final long one_bil = 1_000_000_000L;
                    long val = Math.abs(random.nextLong()) % one_bil;
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : val * one_bil);
                }
            }, BarrageColumnRoundTripTest::longIdentityValidator);
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
            final int j = random.nextInt(20) - 1;
            if (j < 0) {
                chunk.set(i, null);
            } else {
                final String[] entry = new String[j];
                for (int k = 0; k < j; ++k) {
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
            final int j = random.nextInt(20) - 1;
            if (j < 0) {
                chunk.set(i, null);
            } else {
                final long[] entry = new long[j];
                for (int k = 0; k < j; ++k) {
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
            final int j = random.nextInt(20) - 1;
            if (j < 0) {
                chunk.set(i, null);
            } else {
                final long[] entry = new long[j];
                for (int k = 0; k < j; ++k) {
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
            final int j = random.nextInt(20) - 1;
            if (j < 0) {
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
            final int j = random.nextInt(20) - 1;
            if (j < 0) {
                chunk.set(i, null);
            } else {
                chunk.set(i, LocalTime.ofNanoOfDay(i * 1700000L));
            }
        }
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

    private enum SpecialMode {
        NONE, MAP, VAR_LEN_LIST, FIXED_LEN_LIST, ZDT, ZDT_WITH_FACTOR
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

            // swiss cheese subset
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
}
