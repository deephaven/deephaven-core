//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.FloatRangeFilter;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.junit.Assert.fail;

/**
 * Cross-checks {@link FloatPushdownHandler} against the engine's own filter semantics for every range operator and
 * every combination of bounds, rather than relying on hand-picked cases.
 * <p>
 * The contract is one-directional: the handler may answer "maybe" whenever it likes, but it may only answer "no" when
 * genuinely no value in the row group can match. So for every (row group, filter) pair, if the handler excludes the row
 * group, no sampled value inside {@code [min, max]} may satisfy the filter's own chunk filter.
 */
@Category(OutOfBandTest.class)
public class FloatRangeExhaustiveTest {

    private static final TableDefinition TABLE_DEFINITION =
            TableDefinition.of(ColumnDefinition.ofFloat("x"));

    private static final float[] INTERESTING = {
            Float.NEGATIVE_INFINITY, -Float.MAX_VALUE / 2, -1.0f, -0.0f, 0.0f, 1.0f, 2.0f, 3.0f,
            Float.MAX_VALUE / 2, Float.POSITIVE_INFINITY};

    private static Statistics<?> floatStats(final float min, final float max) {
        final PrimitiveType col = Types.required(FLOAT).named("x");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(Float.floatToIntBits(min)))
                .withMax(BytesUtils.intToBytes(Float.floatToIntBits(max)))
                .withNumNulls(0L)
                .build();
    }

    private static List<Float> valuesWithin(final float min, final float max) {
        final List<Float> out = new ArrayList<>();
        for (final float candidate : INTERESTING) {
            if (candidate >= min && candidate <= max) {
                out.add(candidate);
            }
        }
        return out;
    }

    @Test
    public void handlerNeverExcludesARowGroupThatCanMatch() {
        final List<Function<Float, FloatRangeFilter>> factories = List.of(
                pivot -> FloatRangeFilter.lt("x", pivot),
                pivot -> FloatRangeFilter.leq("x", pivot),
                pivot -> FloatRangeFilter.gt("x", pivot),
                pivot -> FloatRangeFilter.geq("x", pivot));
        final List<String> names = List.of("lt", "leq", "gt", "geq");

        int excluded = 0;
        for (final float min : INTERESTING) {
            for (final float max : INTERESTING) {
                if (min > max) {
                    continue;
                }
                final Statistics<?> stats = floatStats(min, max);
                final List<Float> present = valuesWithin(min, max);
                for (int f = 0; f < factories.size(); f++) {
                    for (final float pivot : INTERESTING) {
                        final FloatRangeFilter filter = factories.get(f).apply(pivot);
                        filter.init(TABLE_DEFINITION);
                        final boolean handlerSaysMaybe = evaluate(filter, stats);
                        if (handlerSaysMaybe) {
                            continue;
                        }
                        excluded++;
                        // The handler excluded the row group; the engine must agree that nothing here matches.
                        final ChunkFilter chunkFilter = filter.chunkFilter().orElseThrow();
                        for (final float value : present) {
                            if (matches(chunkFilter, value)) {
                                fail(String.format(
                                        "handler excluded row group [%s, %s] for %s(%s), but %s matches",
                                        min, max, names.get(f), pivot, value));
                            }
                        }
                    }
                }
            }
        }
        // Guard against a vacuous pass: the handler must actually be excluding things.
        org.junit.Assert.assertTrue("expected some exclusions, got " + excluded, excluded > 100);
    }

    private static boolean matches(final ChunkFilter chunkFilter, final float value) {
        final io.deephaven.chunk.WritableFloatChunk<io.deephaven.chunk.attributes.Values> chunk =
                io.deephaven.chunk.WritableFloatChunk.makeWritableChunk(1);
        try (final io.deephaven.util.SafeCloseable ignored = chunk::close) {
            chunk.set(0, value);
            chunk.setSize(1);
            final io.deephaven.chunk.WritableLongChunk<io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys> keys =
                    io.deephaven.chunk.WritableLongChunk.makeWritableChunk(1);
            keys.set(0, 0L);
            keys.setSize(1);
            final io.deephaven.chunk.WritableLongChunk<io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys> out =
                    io.deephaven.chunk.WritableLongChunk.makeWritableChunk(1);
            out.setSize(0);
            chunkFilter.filter(chunk, keys, out);
            final boolean matched = out.size() > 0;
            keys.close();
            out.close();
            return matched;
        }
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.maybeMakeForFilter} does per location.
     */
    private static boolean evaluate(final FloatRangeFilter filter, final Statistics<?> stats) {
        return FloatPushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}
