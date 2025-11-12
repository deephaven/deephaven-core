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
public abstract class QueryTableMinAggTestBase {

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

    public abstract Table min(char[] source);

    public abstract Table min(float[] source);

    public abstract Table min(double[] source);

    @Test
    public void testEmptyChar() {
        assertThat(min(new char[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyFloat() {
        assertThat(min(new float[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyDouble() {
        assertThat(min(new double[0]).isEmpty()).isTrue();
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
    public void testSkipsNullChar() {
        check((char) (Character.MAX_VALUE - 1), new char[] {QueryConstants.NULL_CHAR, Character.MAX_VALUE - 1});
    }

    @Test
    public void testSkipsNullFloat() {
        check(Float.POSITIVE_INFINITY, new float[] {QueryConstants.NULL_FLOAT, Float.POSITIVE_INFINITY});
    }

    @Test
    public void testSkipsNullDouble() {
        check(Double.POSITIVE_INFINITY, new double[] {QueryConstants.NULL_DOUBLE, Double.POSITIVE_INFINITY});
    }

    @Test
    public void testSkipsNanFloat() {
        check(Float.POSITIVE_INFINITY, new float[] {Float.POSITIVE_INFINITY, Float.NaN});
    }

    @Test
    public void testSkipsNanDouble() {
        check(Double.POSITIVE_INFINITY, new double[] {Double.POSITIVE_INFINITY, Double.NaN});
    }

    @Test
    public void testNegZeroFirstFloat() {
        check(-0.0f, new float[] {-0.0f, 0.0f});
    }

    @Test
    public void testNegZeroFirstDouble() {
        check(-0.0, new double[] {-0.0, 0.0});
    }

    @Test
    public void testZeroFirstFloat() {
        check(0.0f, new float[] {0.0f, -0.0f});
    }

    @Test
    public void testZeroFirstDouble() {
        check(0.0, new double[] {0.0, -0.0});
    }

    @Test
    public void testAllNaNFloat() {
        check(QueryConstants.NULL_FLOAT, new float[] {Float.NaN});
    }

    @Test
    public void testAllNaNDouble() {
        check(QueryConstants.NULL_DOUBLE, new double[] {Double.NaN});
    }

    public void check(char expected, char[] data) {
        assertThat(min(data))
                .columnSourceValues(S1)
                .chars()
                .containsExactly(expected);
    }

    public void check(float expected, float[] data) {
        assertThat(min(data))
                .columnSourceValues(S1)
                .floats()
                .usingElementComparator(Float::compare)
                .containsExactly(expected);
    }

    public void check(double expected, double[] data) {
        assertThat(min(data))
                .columnSourceValues(S1)
                .doubles()
                .usingElementComparator(Double::compare)
                .containsExactly(expected);
    }
}
