/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.RangeJoinMatch;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTime;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Array;
import java.util.*;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.table.TableFactory.emptyTable;
import static io.deephaven.engine.testutil.TstUtils.sparsify;
import static io.deephaven.time.DateTimeUtils.minus;
import static io.deephaven.time.DateTimeUtils.plus;
import static io.deephaven.util.QueryConstants.*;
import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for the {@link QueryTable#rangeJoin(Table, Collection, RangeJoinMatch, Collection)} and related delegation
 * from {@link io.deephaven.api.TableOperations#rangeJoin(Object, Collection, Collection)}.
 */
@Category(OutOfBandTest.class)
public class QueryTableRangeJoinTest {

    private static final boolean PRINT_EXPECTED_EXCEPTIONS = false;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    // region validation tests

    @Test
    public void testRefreshingUnsupported() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=II % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=II % 5", "RRV=ii / 0.3");
        final Runnable test = () -> lt.rangeJoin(rt, List.of("BB", "LSV < RRV < LEV"), List.of(AggGroup("II")));

        // {static, static} works
        test.run();

        // {refreshing, static} fails
        lt.setRefreshing(true);
        expectException(test, UnsupportedOperationException.class);

        // {refreshing, refreshing} fails
        rt.setRefreshing(true);
        expectException(test, UnsupportedOperationException.class);

        // {static, refreshing} fails
        lt.setRefreshing(false);
        expectException(test, UnsupportedOperationException.class);
    }

    @Test
    public void testUnsupportedAggregations() {
        final Aggregation[] unsupportedAggs = new Aggregation[] {
                AggAbsSum("II"),
                AggApproxPct(0.1, "II"),
                AggAvg("II"),
                AggCount("Count"),
                AggCountDistinct("II"),
                AggDistinct("II"),
                AggFirst("II"),
                AggFirstRowKey("FRK"),
                AggFormula("each", "each", "II"),
                AggFreeze("II"),
                AggLast("II"),
                AggLastRowKey("LRK"),
                AggMax("II"),
                AggMed("II"),
                AggMin("II"),
                AggPartition("Constituents"),
                AggPct(0.1, "II"),
                AggSortedFirst("BB", "II"),
                AggSortedLast("BB", "II"),
                AggStd("II"),
                AggSum("II"),
                AggTDigest("II"),
                AggUnique("II"),
                AggVar("II"),
                AggWAvg("BB", "II"),
                AggWSum("BB", "II")
        };

        final Table lt = emptyTable(100).updateView("II=ii", "BB=II % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=II % 5", "RRV=ii / 0.3");

        for (final Aggregation agg : unsupportedAggs) {
            expectException(
                    () -> lt.rangeJoin(rt, List.of("BB", "LSV < RRV < LEV"), List.of(agg)),
                    UnsupportedOperationException.class);
        }
    }

    @Test
    public void testInvalidExactMatches() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=i % 5", "RRV=ii / 0.3");
        final Runnable[] tests = new Runnable[] {
                // Missing both
                () -> lt.rangeJoin(rt, List.of("WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left
                () -> lt.rangeJoin(rt, List.of("WRONG=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right
                () -> lt.rangeJoin(rt, List.of("BB=WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both after valid
                () -> lt.rangeJoin(rt, List.of("II", "WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left after valid
                () -> lt.rangeJoin(rt, List.of("II", "WRONG=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right after valid
                () -> lt.rangeJoin(rt, List.of("II", "BB=WRONG", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both before valid
                () -> lt.rangeJoin(rt, List.of("WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left before valid
                () -> lt.rangeJoin(rt, List.of("WRONG=BB", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right before valid
                () -> lt.rangeJoin(rt, List.of("BB=WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both and bad data type
                () -> lt.rangeJoin(rt, List.of("WRONG", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left and bad data type
                () -> lt.rangeJoin(rt, List.of("WRONG=BB", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right and bad data type
                () -> lt.rangeJoin(rt, List.of("BB=WRONG", "II=BB", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing both and bad component type
                () -> lt.updateView("II=Long.toString(II)").groupBy("BB")
                        .rangeJoin(rt.updateView("II=(CharSequence) Long.toString(II)").groupBy("BB"),
                                List.of("WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing left and bad component type (none for left)
                () -> lt.rangeJoin(rt.updateView("II=(CharSequence) Long.toString(II)").groupBy("BB"),
                        List.of("WRONG=BB", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Missing right and bad component type (none for right)
                () -> lt.updateView("II=Long.toString(II)").groupBy("BB")
                        .rangeJoin(rt, List.of("BB=WRONG", "II", "LSV < RRV < LEV"), List.of(AggGroup("II"))),
        };
        for (final Runnable test : tests) {
            expectException(test, IllegalArgumentException.class);
        }
    }

    @Test
    public void testInvalidRangeMatch() {
        final Table lt = emptyTable(100).updateView("II=ii", "BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
        final Table rt = emptyTable(100).updateView("II=ii", "BB=i % 5", "RRV=ii / 0.3");
        final Runnable[] tests = new Runnable[] {
                // Missing all
                () -> lt.rangeJoin(rt, List.of("WRONG1 < WRONG < WRONG2"), List.of(AggGroup("II"))),
                // Incompatible types
                () -> lt.rangeJoin(rt.updateView("RRV=(int) RRV"), List.of("LSV < RRV < LEV"), List.of(AggGroup("II"))),
                // Invalid type
                () -> lt.updateView("LSV=new double[] {LSV}", "LEV=new double[] {LEV}")
                        .rangeJoin(rt.updateView("RRV = new double[] {RRV}"),
                                List.of("LSV < RRV < LEV"), List.of(AggGroup("II"))),
        };
        for (final Runnable test : tests) {
            expectException(test, IllegalArgumentException.class);
        }
    }

    private static void expectException(
            @NotNull final Runnable test,
            @NotNull final Class<? extends Exception> exceptionClass) {
        try {
            test.run();
            failBecauseExceptionWasNotThrown(exceptionClass);
        } catch (Throwable expected) {
            assertThat(expected).isInstanceOf(exceptionClass);
            if (PRINT_EXPECTED_EXCEPTIONS) {
                expected.printStackTrace(System.out);
            }
        }
    }

    // endregion validation tests

    private static final DateTime NOW = DateTime.now();

    private enum Type {
        // @formatter:off
        CHAR(ChunkType.Char, "char", "CharVector", "io.deephaven.vector.CharVectorDirect.ZERO_LENGTH_VECTOR",
                new char[]{NULL_CHAR, '?', 'B', 'D', 'F', 'H', 'J'},
                new char[]{NULL_CHAR, 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'}),
        BYTE(ChunkType.Byte, "byte", "ByteVector", "io.deephaven.vector.ByteVectorDirect.ZERO_LENGTH_VECTOR",
                new byte[]{NULL_BYTE, -1, 1, 3, 5, 7, 9},
                new byte[]{NULL_BYTE, 0, 1, 2, 3, 4, 5, 6, 7, 8}),
        SHORT(ChunkType.Short, "short", "ShortVector", "io.deephaven.vector.ShortVectorDirect.ZERO_LENGTH_VECTOR",
                new short[]{NULL_SHORT, -1, 1, 3, 5, 7, 9},
                new short[]{NULL_SHORT, 0, 1, 2, 3, 4, 5, 6, 7, 8}),
        INT(ChunkType.Int, "int", "IntVector", "io.deephaven.vector.IntVectorDirect.ZERO_LENGTH_VECTOR",
                new int[]{NULL_INT, -1, 1, 3, 5, 7, 9 },
                new int[]{NULL_INT, 0, 1, 2, 3, 4, 5, 6, 7, 8}),
        LONG(ChunkType.Long, "long", "LongVector", "io.deephaven.vector.LongVectorDirect.ZERO_LENGTH_VECTOR",
                new long[]{NULL_LONG, -1, 1, 3, 5, 7, 9},
                new long[]{NULL_LONG, 0, 1, 2, 3, 4, 5, 6, 7, 8}),
        FLOAT(ChunkType.Float, "float", "FloatVector", "io.deephaven.vector.FloatVectorDirect.ZERO_LENGTH_VECTOR",
                new float[]{NULL_FLOAT, -1, 1, 3, 5, 7, 9, Float.NaN},
                new float[]{NULL_FLOAT, 0, 1, 2, 3, 4, 5, 6, 7, 8, Float.NaN}),
        DOUBLE(ChunkType.Double, "double", "DoubleVector", "io.deephaven.vector.DoubleVectorDirect.ZERO_LENGTH_VECTOR",
                new double[]{NULL_DOUBLE, -1, 1, 3, 5, 7, 9, Double.NaN},
                new double[]{NULL_DOUBLE, 0, 1, 2, 3, 4, 5, 6, 7, 8, Double.NaN}),
        TIMESTAMP(ChunkType.Object, "DateTime", "ObjectVector", "io.deephaven.vector.ObjectVectorDirect.empty()",
                new DateTime[]{null, minus(NOW, 1), plus(NOW, 1), plus(NOW, 3), plus(NOW, 5), plus(NOW, 7),
                        plus(NOW, 9)},
                new DateTime[]{null, NOW, plus(NOW, 1), plus(NOW, 2), plus(NOW, 3), plus(NOW, 4), plus(NOW, 5),
                        plus(NOW, 6), plus(NOW, 7), plus(NOW, 8)}),
        STRING(ChunkType.Object, "String", "ObjectVector", "io.deephaven.vector.ObjectVectorDirect.empty()",
                new String[]{null, ">?@", "DEF", "IJK", "OPQ", "UVW", "[\\]"},
                new String[]{null, "ABC", "DEF", "FGH", "IJK", "LMN", "OPQ", "RST", "UVW", "XYZ"});

        private static final int[] LEFT_OFFSETS    = new int[] {0, 1,        2, 3, 4, 5, 6,        7       };
        private static final int[] EXPECTED_LT     = new int[] {0, 1,        3, 5, 7, 9, NULL_INT, NULL_INT};
        private static final int[] EXPECTED_LEQ    = new int[] {0, 1,        2, 4, 6, 8, NULL_INT, NULL_INT};
        private static final int[] EXPECTED_LEQAP  = new int[] {0, 0,        2, 4, 6, 8, 9,        NULL_INT};
        private static final int[] EXPECTED_GT     = new int[] {9, NULL_INT, 1, 3, 5, 7, 9,        NULL_INT};
        private static final int[] EXPECTED_GEQ    = new int[] {9, NULL_INT, 2, 4, 6, 8, 9,        NULL_INT};
        private static final int[] EXPECTED_GEQAF  = new int[] {9, 1,        2, 4, 6, 8, 9,        NULL_INT};
        // @formatter:on

        private final ChunkType chunkType;
        private final String className;
        private final String vectorName;
        private final String emptyVector;

        private final Object uniqueLeftValues;
        private final int uniqueLeftCount;
        private final Object uniqueRightValues;
        private final int uniqueRightCount;

        Type(@NotNull final ChunkType chunkType,
                @NotNull final String className,
                @NotNull final String vectorName,
                @NotNull final String emptyVector,
                @NotNull final Object uniqueLeftValues,
                @NotNull final Object uniqueRightValues) {
            this.chunkType = chunkType;
            this.className = className;
            this.vectorName = vectorName;
            this.emptyVector = emptyVector;
            this.uniqueLeftValues = uniqueLeftValues;
            uniqueLeftCount = Array.getLength(uniqueLeftValues);
            this.uniqueRightValues = uniqueRightValues;
            uniqueRightCount = Array.getLength(uniqueRightValues);
        }

        private Object repeat(@NotNull final Object values, final int targetSize, final boolean sort) {
            final int valuesLength = Array.getLength(values);
            final int nCopies = (targetSize + valuesLength - 1) / valuesLength;
            final int resultSize = nCopies * valuesLength;
            final Object repeated = Array.newInstance(values.getClass().getComponentType(), resultSize;
            for (int ii = 0; ii < nCopies; ++ii) {
                // noinspection SuspiciousSystemArraycopy
                System.arraycopy(values, 0, repeated, ii * valuesLength, valuesLength);
            }
            if (sort) {
                //noinspection resource
                chunkType.writableChunkWrap(repeated, 0, resultSize).sort();
            }
            return repeated;
        }
    }

    @Test
    public void testLtGt() {
        QueryScope.addParam("expectedStartIndices", Type.EXPECTED_LT);
        QueryScope.addParam("expectedEndIndices", Type.EXPECTED_GT);
        for (final Type type : Type.values()) {
            QueryScope.addParam("ulCount", type.uniqueLeftCount);
            QueryScope.addParam("ulValues", type.uniqueLeftValues);
            QueryScope.addParam("urCount", type.uniqueRightCount);
            QueryScope.addParam("urValues", type.uniqueRightValues);
            final Table lt = emptyTable(200_000L).updateView(
                    // 160 buckets, 100 size 1, 20 size 100, and 40 sized a little under 5000
                    "BB=ii < 100 ? ii : ii < 2100 ? ii % 20 + 100 : ii % 40 + 110",
                    "LSI=ii % ulCount",
                    "LSV=ulValues[LSI]",
                    "ExpectedFirstIndex=expectedStartIndices[LSI]",
                    "LEI=(ii * 2 + ii % 2) % ulCount",
                    "LEV=ulValues[LEI]",
                    "ExpectedLastIndex=expectedEndIndices[LSI]");
            final Table ltSparse = sparsify(lt, 2);
            final Table rtSingles = emptyTable(160L * type.uniqueRightCount).updateView(
                    // All unique right values, repeated once per bucket
                    "BB=ii / urCount",
                    "RRI=ii % urCount",
                    "RRV=urValues[RRI]");
            final Table rtDoublesAndEmpties = emptyTable(160L * type.uniqueRightCount).updateView(
                    // All unique right values, repeated twice per even bucket, odd buckets all null and thus empty
                    "BB=ii / (2 * urCount)",
                    "RRI=BB % 2 == 0 ? (ii / 2) % urCount : 0",
                    "RRV=urValues[RRI]");
            final Table rtGrowing = emptyTable((20L * 2 + 140L * 20) * type.uniqueRightCount).updateView(
                    // All unique right values, repeated twice for early buckets and twenty times for later buckets
                    "BB=ii < (20 * 2 * urCount) ? ii / (2 * urCount) : ii / (20 * urCount) + 20",
                    "RRI=BB < 20 ? (ii / 2) % urCount : (ii / 20) % urCount",
                    "RRV=urValues[RRI]");
        }
    }

    @Test
    public void testEmptyRight() {
        for (final Type type : Type.values()) {
            final Table lt = emptyTable(20_000).update("BB=i % 5", "LSV=ii / 0.7", "LEV=ii / 0.1");
            final Table rt = emptyTable(0).update("II=(" + type.className + ") ii", "BB=i % 5", "RRV=ii / 0.3");
            final Table rj = lt.rangeJoin(rt, List.of("BB", "LSV <= RRV <= LEV"), List.of(AggGroup("II")));
            TstUtils.assertTableEquals(rj, lt.update("II=" + type.emptyVector));
        }
    }

    // TODO:
    // 1. Test all types
    // 2. Test NaN filtering for floats and doubles
    // 3. Test empty rights
    // 4. Test re-sizing lefts
    // 5. Test re-sizing rights
    // 6. Test all start rules
    // 7. Test all end rules
    // 8. Test singleton and multiple timestamps


}
