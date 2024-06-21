//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.common.io.LittleEndianDataInputStream;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
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
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.vector.LongVector;
import io.deephaven.vector.LongVectorDirect;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.LongStream;

public class BarrageColumnRoundTripTest extends RefreshingTableTestCase {

    private static final BarrageSubscriptionOptions OPT_DEFAULT_DH_NULLS =
            BarrageSubscriptionOptions.builder()
                    .useDeephavenNulls(true)
                    .build();
    private static final BarrageSubscriptionOptions OPT_DEFAULT = BarrageSubscriptionOptions.builder()
            .build();

    private static final BarrageSubscriptionOptions[] options = new BarrageSubscriptionOptions[] {
            OPT_DEFAULT_DH_NULLS,
            OPT_DEFAULT
    };

    public void testCharChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, char.class, (utO) -> {
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
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, boolean.class, (utO) -> {
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

    public void testByteChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, byte.class, (utO) -> {
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
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, short.class, (utO) -> {
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
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, int.class, (utO) -> {
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
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, long.class, (utO) -> {
                final WritableLongChunk<Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, (utO, utC, subset, offset) -> {
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
            });
        }
    }

    public void testFloatChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, float.class, (utO) -> {
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
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, double.class, (utO) -> {
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
        for (final BarrageSubscriptionOptions opts : options) {
            testRoundTripSerialization(opts, Instant.class, (utO) -> {
                final WritableObjectChunk<Instant, Values> chunk = utO.asWritableObjectChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? null : Instant.ofEpochSecond(0, random.nextLong()));
                }
            }, new ObjectIdentityValidator<>());
        }
    }

    public void testObjectSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, Object.class, initObjectChunk(Integer::toString),
                new ObjectIdentityValidator<>());
    }

    public void testStringSerializationDHNulls() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT_DH_NULLS, String.class, initObjectChunk(Integer::toString),
                new ObjectIdentityValidator<>());
    }

    public void testStringSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, Object.class, initObjectChunk(Integer::toString),
                new ObjectIdentityValidator<>());
    }

    public void testUniqueToStringSerializationDHNulls() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT_DH_NULLS, Unique.class, initObjectChunk(Unique::new),
                new ObjectToStringValidator<>());
    }

    public void testUniqueToStringSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, Unique.class, initObjectChunk(Unique::new),
                new ObjectToStringValidator<>());
    }

    public void testStringArrayDHNullsSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT_DH_NULLS, String[].class,
                BarrageColumnRoundTripTest::initStringArrayChunk, new ObjectIdentityValidator<>());
    }

    public void testStringArraySerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, String[].class, BarrageColumnRoundTripTest::initStringArrayChunk,
                new ObjectIdentityValidator<>());
    }

    public void testLongArraySerializationDHNulls() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT_DH_NULLS, long[].class, BarrageColumnRoundTripTest::initLongArrayChunk,
                new LongArrayIdentityValidator());
    }

    public void testLongArraySerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, long[].class, BarrageColumnRoundTripTest::initLongArrayChunk,
                new LongArrayIdentityValidator());
    }

    public void testLongVectorSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, LongVector.class, BarrageColumnRoundTripTest::initLongVectorChunk,
                new LongVectorIdentityValidator());
    }

    public void testLocalDateVectorSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, LocalDate.class, BarrageColumnRoundTripTest::initLocalDateVectorChunk,
                new LocalDateVectorIdentityValidator());
    }

    public void testLocalTimeVectorSerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, LocalTime.class, BarrageColumnRoundTripTest::initLocalTimeVectorChunk,
                new LocalTimeVectorIdentityValidator());
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

    private static void initLocalDateVectorChunk(final WritableChunk<Values> untypedChunk) {
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

    private static void initLocalTimeVectorChunk(final WritableChunk<Values> untypedChunk) {
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

    private static final class LocalDateVectorIdentityValidator implements Validator {
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

    private static final class LocalTimeVectorIdentityValidator implements Validator {
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

    private static <T> void testRoundTripSerialization(
            final BarrageSubscriptionOptions options, final Class<T> type,
            final Consumer<WritableChunk<Values>> initData, final Validator validator) throws IOException {
        final ChunkType chunkType;
        if (type == Boolean.class || type == boolean.class) {
            chunkType = ChunkType.Byte;
        } else {
            chunkType = ChunkType.fromElementType(type);
        }

        final WritableChunk<Values> srcData = chunkType.makeWritableChunk(4096);
        initData.accept(srcData);

        // The generator owns data; it is allowed to close it prematurely if the data needs to be converted to primitive
        final WritableChunk<Values> data = chunkType.makeWritableChunk(4096);
        data.copyFromChunk(srcData, 0, 0, srcData.size());

        try (SafeCloseable ignored = data;
                ChunkInputStreamGenerator generator = ChunkInputStreamGenerator.makeInputStreamGenerator(
                        chunkType, type, type.getComponentType(), srcData, 0)) {
            // full sub logic
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream();
                    final ChunkInputStreamGenerator.DrainableColumn column =
                            generator.getInputStream(options, null)) {


                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final LongStream.Builder bufferNodes = LongStream.builder();
                column.visitBuffers(bufferNodes::add);
                column.drainTo(baos);
                final DataInput dis =
                        new LittleEndianDataInputStream(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                try (final WritableChunk<Values> rtData =
                        ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                chunkType, type, type.getComponentType(), fieldNodes.iterator(),
                                bufferNodes.build().iterator(),
                                dis, null, 0, 0)) {
                    Assert.eq(data.size(), "data.size()", rtData.size(), "rtData.size()");
                    validator.assertExpected(data, rtData, null, 0);
                }
            }

            // empty subset
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream();
                    final ChunkInputStreamGenerator.DrainableColumn column =
                            generator.getInputStream(options, RowSetFactory.empty())) {

                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final LongStream.Builder bufferNodes = LongStream.builder();
                column.visitBuffers(bufferNodes::add);
                column.drainTo(baos);
                final DataInput dis =
                        new LittleEndianDataInputStream(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                try (final WritableChunk<Values> rtData =
                        ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                chunkType, type, type.getComponentType(), fieldNodes.iterator(),
                                bufferNodes.build().iterator(),
                                dis, null, 0, 0)) {
                    Assert.eq(rtData.size(), "rtData.size()", 0);
                }
            }

            // swiss cheese subset
            final Random random = new Random(0);
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            for (int i = 0; i < data.size(); ++i) {
                if (random.nextBoolean()) {
                    builder.appendKey(i);
                }
            }
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream();
                    final RowSet subset = builder.build();
                    final ChunkInputStreamGenerator.DrainableColumn column =
                            generator.getInputStream(options, subset)) {

                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final LongStream.Builder bufferNodes = LongStream.builder();
                column.visitBuffers(bufferNodes::add);
                column.drainTo(baos);
                final DataInput dis =
                        new LittleEndianDataInputStream(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                try (final WritableChunk<Values> rtData =
                        ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                chunkType, type, type.getComponentType(), fieldNodes.iterator(),
                                bufferNodes.build().iterator(),
                                dis, null, 0, 0)) {
                    Assert.eq(subset.intSize(), "subset.intSize()", rtData.size(), "rtData.size()");
                    validator.assertExpected(data, rtData, subset, 0);
                }
            }

            // test append to existing chunk logic
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream();
                    final ChunkInputStreamGenerator.DrainableColumn column =
                            generator.getInputStream(options, null)) {

                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final LongStream.Builder bufferNodes = LongStream.builder();
                column.visitBuffers(bufferNodes::add);
                final long[] buffers = bufferNodes.build().toArray();
                column.drainTo(baos);

                // first message
                DataInput dis = new LittleEndianDataInputStream(
                        new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                try (final WritableChunk<Values> rtData =
                        ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                chunkType, type, type.getComponentType(), fieldNodes.iterator(),
                                Arrays.stream(buffers).iterator(),
                                dis, null, 0, data.size() * 2)) {
                    // second message
                    dis = new LittleEndianDataInputStream(
                            new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                    final WritableChunk<Values> rtData2 =
                            ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                    chunkType, type, type.getComponentType(), fieldNodes.iterator(),
                                    Arrays.stream(buffers).iterator(),
                                    dis, rtData, data.size(), data.size() * 2);
                    Assert.eq(rtData, "rtData", rtData2, "rtData2");
                    validator.assertExpected(data, rtData, null, 0);
                    validator.assertExpected(data, rtData, null, data.size());
                }
            }
        }
    }
}
