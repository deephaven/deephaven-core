//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.assertj.TableAssert.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

@Category(OutOfBandTest.class)
public abstract class QueryTableSortedLastAggTestBase {

    static final String S1 = "S1";

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

    public abstract Table sortedLast(char[] source);

    public abstract Table sortedLast(float[] source);

    public abstract Table sortedLast(double[] source);

    @Test
    public void testEmptyChar() {
        assertThat(sortedLast(new char[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyFloat() {
        assertThat(sortedLast(new float[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyDouble() {
        assertThat(sortedLast(new double[0]).isEmpty()).isTrue();
    }

    @Test
    public void testAllNullChar() {
        check(QueryConstants.NULL_CHAR, new char[] {QueryConstants.NULL_CHAR});
    }

    @Test
    public void testAllNullFloat() {
        check(QueryConstants.NULL_FLOAT, new float[] {QueryConstants.NULL_FLOAT});
    }

    @Test
    public void testAllNullDouble() {
        check(QueryConstants.NULL_DOUBLE, new double[] {QueryConstants.NULL_DOUBLE});
    }

    @Test
    public void testAllNaNFloat() {
        check(Float.NaN, new float[] {Float.NaN});
    }

    @Test
    public void testAllNaNDouble() {
        check(Double.NaN, new double[] {Double.NaN});
    }

    @Test
    public void testValuesChar() {
        check((char) (Character.MAX_VALUE - 1),
                new char[] {QueryConstants.NULL_CHAR, Character.MIN_VALUE, Character.MAX_VALUE - 1});
    }

    @Test
    public void testValuesFloat() {
        check(Float.NaN,
                new float[] {QueryConstants.NULL_FLOAT, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NaN});
    }

    @Test
    public void testValuesDouble() {
        check(Double.NaN, new double[] {QueryConstants.NULL_DOUBLE, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY,
                Double.NaN});
    }

    @Test
    public void testNegZeroFirstFloat() {
        check(0.0f, new float[] {-0.0f, 0.0f});
    }

    @Test
    public void testNegZeroFirstDouble() {
        check(0.0, new double[] {-0.0, 0.0});
    }

    @Test
    public void testZeroFirstFloat() {
        check(-0.0f, new float[] {0.0f, -0.0f});
    }

    @Test
    public void testZeroFirstDouble() {
        check(-0.0, new double[] {0.0, -0.0});
    }

    public void check(char expected, char[] data) {
        assertThat(sortedLast(data))
                .columnSourceValues(S1)
                .chars()
                .containsExactly(expected);
    }

    public void check(float expected, float[] data) {
        assertThat(sortedLast(data))
                .columnSourceValues(S1)
                .floats()
                .usingElementComparator(Float::compare)
                .containsExactly(expected);
    }

    public void check(double expected, double[] data) {
        assertThat(sortedLast(data))
                .columnSourceValues(S1)
                .doubles()
                .usingElementComparator(Double::compare)
                .containsExactly(expected);
    }
}
