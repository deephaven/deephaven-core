/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.barrage.chunk;

import com.google.common.io.LittleEndianDataInputStream;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.LiveTableTestCase;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.sources.chunk.WritableByteChunk;
import io.deephaven.db.v2.sources.chunk.WritableCharChunk;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.sources.chunk.WritableDoubleChunk;
import io.deephaven.db.v2.sources.chunk.WritableFloatChunk;
import io.deephaven.db.v2.sources.chunk.WritableIntChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.sources.chunk.WritableObjectChunk;
import io.deephaven.db.v2.sources.chunk.WritableShortChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.IntFunction;

public class BarrageColumnRoundTripTest extends LiveTableTestCase {

    private static final ChunkInputStreamGenerator.Options OPT_DEFAULT_DH_NULLS =
            new ChunkInputStreamGenerator.Options.Builder()
                    .setUseDeephavenNulls(true)
                    .build();
    private static final ChunkInputStreamGenerator.Options OPT_DEFAULT = new ChunkInputStreamGenerator.Options.Builder()
            .build();

    private static final ChunkInputStreamGenerator.Options[] options = new ChunkInputStreamGenerator.Options[] {
            OPT_DEFAULT_DH_NULLS,
            OPT_DEFAULT
    };

    public void testCharChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, char.class, (utO) -> {
                final WritableCharChunk<Attributes.Values> chunk = utO.asWritableCharChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_CHAR : (char) random.nextInt());
                }
            }, (utO, utC, subset) -> {
                final WritableCharChunk<Attributes.Values> original = utO.asWritableCharChunk();
                final WritableCharChunk<Attributes.Values> computed = utC.asWritableCharChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
        }
    }

    public void testByteChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, byte.class, (utO) -> {
                final WritableByteChunk<Attributes.Values> chunk = utO.asWritableByteChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_BYTE : (byte) random.nextInt());
                }
            }, (utO, utC, subset) -> {
                final WritableByteChunk<Attributes.Values> original = utO.asWritableByteChunk();
                final WritableByteChunk<Attributes.Values> computed = utC.asWritableByteChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
        }
    }

    public void testShortChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, short.class, (utO) -> {
                final WritableShortChunk<Attributes.Values> chunk = utO.asWritableShortChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_SHORT : (short) random.nextInt());
                }
            }, (utO, utC, subset) -> {
                final WritableShortChunk<Attributes.Values> original = utO.asWritableShortChunk();
                final WritableShortChunk<Attributes.Values> computed = utC.asWritableShortChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
        }
    }

    public void testIntChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, int.class, (utO) -> {
                final WritableIntChunk<Attributes.Values> chunk = utO.asWritableIntChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_INT : random.nextInt());
                }
            }, (utO, utC, subset) -> {
                final WritableIntChunk<Attributes.Values> original = utO.asWritableIntChunk();
                final WritableIntChunk<Attributes.Values> computed = utC.asWritableIntChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
        }
    }

    public void testLongChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, long.class, (utO) -> {
                final WritableLongChunk<Attributes.Values> chunk = utO.asWritableLongChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_LONG : random.nextLong());
                }
            }, (utO, utC, subset) -> {
                final WritableLongChunk<Attributes.Values> original = utO.asWritableLongChunk();
                final WritableLongChunk<Attributes.Values> computed = utC.asWritableLongChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
        }
    }

    public void testFloatChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, float.class, (utO) -> {
                final WritableFloatChunk<Attributes.Values> chunk = utO.asWritableFloatChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_FLOAT : random.nextFloat());
                }
            }, (utO, utC, subset) -> {
                final WritableFloatChunk<Attributes.Values> original = utO.asWritableFloatChunk();
                final WritableFloatChunk<Attributes.Values> computed = utC.asWritableFloatChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
        }
    }

    public void testDoubleChunkSerialization() throws IOException {
        final Random random = new Random(0);
        for (final ChunkInputStreamGenerator.Options opts : options) {
            testRoundTripSerialization(opts, double.class, (utO) -> {
                final WritableDoubleChunk<Attributes.Values> chunk = utO.asWritableDoubleChunk();
                for (int i = 0; i < chunk.size(); ++i) {
                    chunk.set(i, i % 7 == 0 ? QueryConstants.NULL_DOUBLE : random.nextDouble());
                }
            }, (utO, utC, subset) -> {
                final WritableDoubleChunk<Attributes.Values> original = utO.asWritableDoubleChunk();
                final WritableDoubleChunk<Attributes.Values> computed = utC.asWritableDoubleChunk();
                if (subset == null) {
                    for (int i = 0; i < original.size(); ++i) {
                        Assert.equals(original.get(i), "original.get(i)", computed.get(i), "computed.get(i)");
                    }
                } else {
                    final MutableInt off = new MutableInt();
                    subset.forAllLongs(key -> Assert.equals(original.get((int) key), "original.get(key)",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())"));
                }
            });
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
                new LongArrayIdentityValidator<>());
    }

    public void testLongArraySerialization() throws IOException {
        testRoundTripSerialization(OPT_DEFAULT, long[].class, BarrageColumnRoundTripTest::initLongArrayChunk,
                new LongArrayIdentityValidator<>());
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

    private static <T> Consumer<WritableChunk<Attributes.Values>> initObjectChunk(final IntFunction<T> mapper) {
        return (untypedChunk) -> {
            final WritableObjectChunk<T, Attributes.Values> chunk = untypedChunk.asWritableObjectChunk();
            for (int i = 0; i < chunk.size(); ++i) {
                chunk.set(i, i % 7 == 0 ? null : mapper.apply(i));
            }
        };
    }

    private static void initStringArrayChunk(final WritableChunk<Attributes.Values> untypedChunk) {
        final Random random = new Random();
        final WritableObjectChunk<String[], Attributes.Values> chunk = untypedChunk.asWritableObjectChunk();

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

    private static void initLongArrayChunk(final WritableChunk<Attributes.Values> untypedChunk) {
        final Random random = new Random();
        final WritableObjectChunk<long[], Attributes.Values> chunk = untypedChunk.asWritableObjectChunk();

        for (int i = 0; i < chunk.size(); ++i) {
            final int j = random.nextInt(20) - 1;
            if (j < 0) {
                chunk.set(i, null);
            } else {
                final long[] entry = new long[j];
                for (int k = 0; k < j; ++k) {
                    entry[k] = i * 10000 + k;
                }
                chunk.set(i, entry);
            }
        }
    }

    private interface Validator {
        void assertExpected(final WritableChunk<Attributes.Values> original,
                final WritableChunk<Attributes.Values> computed,
                @Nullable final OrderedKeys subset);
    }

    private static final class ObjectIdentityValidator<T> implements Validator {
        @Override
        public void assertExpected(final WritableChunk<Attributes.Values> untypedOriginal,
                final WritableChunk<Attributes.Values> unTypedComputed,
                @Nullable OrderedKeys subset) {
            final WritableObjectChunk<T, Attributes.Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<T, Attributes.Values> computed = unTypedComputed.asWritableObjectChunk();

            if (subset == null) {
                subset = Index.CURRENT_FACTORY.getFlatIndex(untypedOriginal.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllLongs(key -> {
                final T next = original.get((int) key);
                if (next == null) {
                    Assert.eqNull(computed.get(off.getAndIncrement()), "computed");
                } else if (next.getClass().isArray()) {
                    // noinspection unchecked
                    final T[] nt = (T[]) next;
                    // noinspection unchecked
                    final T[] ct = (T[]) computed.get(off.getAndIncrement());
                    Assert.equals(nt.length, "nt.length", ct.length, "ct.length");
                    for (int k = 0; k < nt.length; ++k) {
                        Assert.equals(nt[k], "nt[k]", ct[k], "ct[k]");
                    }
                } else {
                    Assert.equals(next, "next", computed.get(off.getAndIncrement()), "computed.get(i)");
                }
            });
        }
    }

    private static class ObjectToStringValidator<T> implements Validator {
        @Override
        public void assertExpected(final WritableChunk<Attributes.Values> untypedOriginal,
                final WritableChunk<Attributes.Values> untypedComputed,
                @Nullable OrderedKeys subset) {
            final WritableObjectChunk<T, Attributes.Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<String, Attributes.Values> computed = untypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = Index.CURRENT_FACTORY.getFlatIndex(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllLongs(key -> {
                if (original.get((int) key) == null) {
                    Assert.eqNull(computed.get(off.getAndIncrement()), "computed");
                } else {
                    Assert.equals(original.get((int) key).toString(), "original.get(key).toString()",
                            computed.get(off.getAndIncrement()), "computed.get(off.getAndIncrement())");
                }
            });
        }
    }

    private static final class LongArrayIdentityValidator<T> implements Validator {
        @Override
        public void assertExpected(final WritableChunk<Attributes.Values> untypedOriginal,
                final WritableChunk<Attributes.Values> unTypedComputed,
                @Nullable OrderedKeys subset) {
            final WritableObjectChunk<long[], Attributes.Values> original = untypedOriginal.asWritableObjectChunk();
            final WritableObjectChunk<long[], Attributes.Values> computed = unTypedComputed.asWritableObjectChunk();
            if (subset == null) {
                subset = Index.CURRENT_FACTORY.getFlatIndex(original.size());
            }
            final MutableInt off = new MutableInt();
            subset.forAllLongs(i -> {
                final long[] nt = original.get((int) i);
                if (nt == null) {
                    Assert.eqNull(computed.get(off.getAndIncrement()), "computed");
                } else {
                    final long[] ct = computed.get(off.getAndIncrement());
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

    @SuppressWarnings("UnstableApiUsage")
    private <T> void testRoundTripSerialization(
            final ChunkInputStreamGenerator.Options options, final Class<T> type,
            final Consumer<WritableChunk<Attributes.Values>> initData, final Validator validator) throws IOException {
        final ChunkType chunkType = ChunkType.fromElementType(type);

        WritableChunk<Attributes.Values> rtData = null;
        final WritableChunk<Attributes.Values> data = chunkType.makeWritableChunk(4096);

        try {
            initData.accept(data);

            final ChunkInputStreamGenerator generator =
                    ChunkInputStreamGenerator.makeInputStreamGenerator(chunkType, type, data);

            // full sub logic
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream()) {
                final ChunkInputStreamGenerator.DrainableColumn column = generator.getInputStream(options, null);
                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final TLongArrayList bufferNodes = new TLongArrayList();
                column.visitBuffers(bufferNodes::add);
                column.drainTo(baos);
                final DataInput dis =
                        new LittleEndianDataInputStream(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));

                rtData = (WritableChunk<Attributes.Values>) ChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, chunkType, type, fieldNodes.iterator(), bufferNodes.iterator(), dis);
                Assert.eq(data.size(), "data.size()", rtData.size(), "rtData.size()");
                validator.assertExpected(data, rtData, null);
            }

            // empty subset
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream()) {
                final ChunkInputStreamGenerator.DrainableColumn column =
                        generator.getInputStream(options, Index.CURRENT_FACTORY.getEmptyIndex());
                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final TLongArrayList bufferNodes = new TLongArrayList();
                column.visitBuffers(bufferNodes::add);
                column.drainTo(baos);
                final DataInput dis =
                        new LittleEndianDataInputStream(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                rtData = (WritableChunk<Attributes.Values>) ChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, chunkType, type, fieldNodes.iterator(), bufferNodes.iterator(), dis);
                Assert.eq(rtData.size(), "rtData.size()", 0);
            }

            // swiss cheese subset
            final Random random = new Random();
            final Index.SequentialBuilder builder = Index.CURRENT_FACTORY.getSequentialBuilder();
            for (int i = 0; i < data.size(); ++i) {
                if (random.nextBoolean()) {
                    builder.appendKey(i);
                }
            }
            try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos =
                    new BarrageProtoUtil.ExposedByteArrayOutputStream();
                    final Index subset = builder.getIndex()) {
                final ChunkInputStreamGenerator.DrainableColumn column = generator.getInputStream(options, subset);
                final ArrayList<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodes = new ArrayList<>();
                column.visitFieldNodes((numElements, nullCount) -> fieldNodes
                        .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount)));
                final TLongArrayList bufferNodes = new TLongArrayList();
                column.visitBuffers(bufferNodes::add);
                column.drainTo(baos);
                final DataInput dis =
                        new LittleEndianDataInputStream(new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
                rtData = (WritableChunk<Attributes.Values>) ChunkInputStreamGenerator.extractChunkFromInputStream(
                        options, chunkType, type, fieldNodes.iterator(), bufferNodes.iterator(), dis);
                Assert.eq(subset.intSize(), "subset.intSize()", rtData.size(), "rtData.size()");
                validator.assertExpected(data, rtData, subset);
            }
        } finally {
            data.close();
            if (rtData != null) {
                rtData.close();
            }
        }
    }
}
