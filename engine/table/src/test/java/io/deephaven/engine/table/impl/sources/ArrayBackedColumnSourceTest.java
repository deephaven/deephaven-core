//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ResettableLongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer.BoxerKernel;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.qst.array.Array;
import io.deephaven.qst.array.GenericArray;
import io.deephaven.qst.array.PrimitiveArray;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.Type;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

public class ArrayBackedColumnSourceTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void fromBooleanArray() {
        check(Type.booleanType(), false, null, true);
    }

    @Test
    public void fromByteArray() {
        check(Type.byteType(), (byte) 1, null, (byte) 3);
    }

    @Test
    public void fromCharArray() {
        check(Type.charType(), '1', null, '3');
    }

    @Test
    public void fromShortArray() {
        check(Type.shortType(), (short) 1, null, (short) 3);
    }

    @Test
    public void fromIntArray() {
        check(Type.intType(), 1, null, 3);
    }

    @Test
    public void fromLongArray() {
        check(Type.longType(), 1L, null, 3L);
    }

    @Test
    public void fromFloatArray() {
        check(Type.floatType(), 1f, null, 3f);
    }

    @Test
    public void fromDoubleArray() {
        check(Type.doubleType(), 1d, null, 3d);
    }

    @Test
    public void fromStringArray() {
        check(Type.stringType(), "1", null, "3");
    }

    @Test
    public void fromInstants() {
        check(Type.instantType(), Instant.ofEpochMilli(1), null, Instant.ofEpochMilli(3));
    }

    /**
     * A source holds no value at a row key past its capacity: each read of such a row key, by key or by chunk, current
     * or previous, ordered or unordered, returns null. That holds for a source never allocated, and for the block after
     * the last block allocated, which the source's array of blocks already has room for.
     */
    @Test
    public void readsPastCapacityAreNull() {
        final List<Supplier<WritableColumnSource<?>>> sourceFactories = List.of(
                ByteArraySource::new,
                CharacterArraySource::new,
                ShortArraySource::new,
                IntegerArraySource::new,
                LongArraySource::new,
                FloatArraySource::new,
                DoubleArraySource::new,
                () -> new ObjectArraySource<>(String.class),
                BooleanArraySource::new,
                () -> (WritableColumnSource<?>) new BooleanArraySource().reinterpret(byte.class),
                InstantArraySource::new,
                () -> new ZonedDateTimeArraySource(ZoneId.of("UTC")));
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (final Supplier<WritableColumnSource<?>> sourceFactory : sourceFactories) {
            final WritableColumnSource<?> unallocated = sourceFactory.get();
            unallocated.startTrackingPrevValues();
            assertReadsNull(unallocated, 0);
            updateGraph.runWithinUnitTestCycle(() -> assertReadsNull(unallocated, 0));

            final WritableColumnSource<?> allocated = sourceFactory.get();
            allocated.startTrackingPrevValues();
            updateGraph.runWithinUnitTestCycle(() -> {
                allocated.ensureCapacity(ArrayBackedColumnSource.BLOCK_SIZE);
                assertReadsNull(allocated, ArrayBackedColumnSource.BLOCK_SIZE);
            });
            assertReadsNull(allocated, ArrayBackedColumnSource.BLOCK_SIZE);
        }
    }

    /** Reads row keys from {@code first}, which is at or past {@code source}'s capacity, and checks each is null. */
    private static void assertReadsNull(final WritableColumnSource<?> source, final long first) {
        final String name = source.getClass().getSimpleName();
        final long blockSize = ArrayBackedColumnSource.BLOCK_SIZE;
        try (final RowSet withinBlock = RowSetFactory.fromRange(first, first + 9);
                final RowSet acrossBlocks = RowSetFactory.fromRange(first, first + 2 * blockSize - 1);
                final RowSet sparse = RowSetFactory.fromKeys(first, first + 3, first + 3 * blockSize + 1)) {
            for (final RowSet rows : List.of(withinBlock, acrossBlocks, sparse)) {
                final int size = rows.intSize();
                try (final ChunkSource.GetContext getContext = source.makeGetContext(size);
                        final ChunkSource.FillContext fillContext = source.makeFillContext(size);
                        final WritableChunk<Values> dest = source.getChunkType().makeWritableChunk(size)) {
                    assertNulls(name + " getChunk", source.getChunk(getContext, rows));
                    assertNulls(name + " getPrevChunk", source.getPrevChunk(getContext, rows));
                    source.fillChunk(fillContext, dest, rows);
                    assertNulls(name + " fillChunk", dest);
                    source.fillPrevChunk(fillContext, dest, rows);
                    assertNulls(name + " fillPrevChunk", dest);
                }
                rows.forAllRowKeys(key -> {
                    assertThat(source.get(key)).as(name + " get " + key).isNull();
                    assertThat(source.getPrev(key)).as(name + " getPrev " + key).isNull();
                });
            }
        }

        assertThat(source).as(name + " fills unordered").isInstanceOf(FillUnordered.class);
        // noinspection unchecked
        final FillUnordered<Values> unordered = (FillUnordered<Values>) source;
        final long[] keys = {first + 5, first, first + 2 * blockSize + 7};
        try (final ChunkSource.FillContext fillContext = source.makeFillContext(keys.length);
                final WritableChunk<Values> dest = source.getChunkType().makeWritableChunk(keys.length);
                final ResettableLongChunk<RowKeys> keyChunk = ResettableLongChunk.makeResettableChunk()) {
            keyChunk.resetFromTypedArray(keys, 0, keys.length);
            unordered.fillChunkUnordered(fillContext, dest, keyChunk);
            assertNulls(name + " fillChunkUnordered", dest);
            unordered.fillPrevChunkUnordered(fillContext, dest, keyChunk);
            assertNulls(name + " fillPrevChunkUnordered", dest);
        }
    }

    private static void assertNulls(final String read, final Chunk<? extends Values> chunk) {
        try (final BoxerKernel boxer = ChunkBoxer.getBoxer(chunk.getChunkType(), chunk.size())) {
            final ObjectChunk<?, ? extends Values> boxed = boxer.box(chunk);
            for (int ii = 0; ii < boxed.size(); ++ii) {
                assertThat(boxed.get(ii)).as(read + " at " + ii).isNull();
            }
        }
    }

    @Test
    public void fromCustomType() {
        check(CustomType.of(MyCustomType.class), MyCustomType.A, null, MyCustomType.B);
    }

    enum MyCustomType {
        A, B
    }

    private static <T> void check(PrimitiveType<T> type, T... values) {
        PrimitiveArray<T> array = PrimitiveArray.of(type, values);
        WritableColumnSource<T> columnSource = ArrayBackedColumnSource.from(array);
        int ix = 0;
        for (T left : values) {
            assertThat(columnSource.get(ix++)).isEqualTo(left);
        }
        check(Objects::equals, type, values);
    }

    private static <T> void check(GenericType<T> type, T... values) {
        check(Objects::equals, type, values);
    }

    private static <T> void check(BiPredicate<T, Object> comparison, GenericType<T> type, T... values) {
        GenericArray<T> array = GenericArray.of(type, values);
        WritableColumnSource<?> columnSource = ArrayBackedColumnSource.from(array);
        int ix = 0;
        for (T left : values) {
            assertThat(columnSource.get(ix++)).matches((Predicate<Object>) right -> comparison.test(left, right));
        }
        check(comparison, (Type<T>) type, values);
    }

    private static <T> void check(BiPredicate<T, Object> comparison, Type<T> type, T... values) {
        Array<T> array = Array.of(type, values);
        WritableColumnSource<?> columnSource = ArrayBackedColumnSource.from(array);
        int ix = 0;
        for (T left : values) {
            assertThat(columnSource.get(ix++)).matches((Predicate<Object>) right -> comparison.test(left, right));
        }
    }
}
