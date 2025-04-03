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
public abstract class QueryTableMedianAggTestBase {

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

    public abstract Table median(char[] source);

    public abstract Table median(float[] source);

    public abstract Table median(double[] source);

    @Test
    public void testEmptyChar() {
        assertThat(median(new char[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyFloat() {
        assertThat(median(new float[0]).isEmpty()).isTrue();
    }

    @Test
    public void testEmptyDouble() {
        assertThat(median(new double[0]).isEmpty()).isTrue();
    }

    @Test
    public void testSplitChar() {
        check('b', new char[] {'a', 'b'});
    }

    @Test
    public void testSplitFloat() {
        check(0.5f, new float[] {0.0f, 1.0f});
    }

    @Test
    public void testSplitDouble() {
        check(0.5, new double[] {0.0, 1.0});
    }

    @Test
    public void testChar() {
        check('b', new char[] {'a', 'b', 'z'});
    }

    @Test
    public void testFloat() {
        check(0.42f, new float[] {0.0f, 0.42f, 1.0f});
    }

    @Test
    public void testDouble() {
        check(0.42, new double[] {0.0, 0.42, 1.0});
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
        check('a', new char[] {QueryConstants.NULL_CHAR, QueryConstants.NULL_CHAR, 'a'});
    }

    @Test
    public void testSkipsNullFloat() {
        check(0.0f, new float[] {QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT, 0.0f});
    }

    @Test
    public void testSkipsNullDouble() {
        check(0.0, new double[] {QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, 0.0});
    }

    @Test
    public void testSkipsNanFloat() {
        check(0.0f, new float[] {Float.NaN, Float.NaN, 0.0f});
    }

    @Test
    public void testSkipsNanDouble() {
        check(0.0, new double[] {Double.NaN, Double.NaN, 0.0});
    }

    @Test
    public void testNegZeroFirstFloat() {
        check(-0.0f, new float[] {-0.0f, 0.0f, 0.0f});
    }

    @Test
    public void testNegZeroFirstDouble() {
        check(-0.0, new double[] {-0.0, 0.0, 0.0});
    }

    @Test
    public void testZeroFirstFloat() {
        check(-0.0f, new float[] {0.0f, -0.0f, -0.0f});
    }

    @Test
    public void testZeroFirstDouble() {
        check(-0.0, new double[] {0.0, -0.0, -0.0});
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
        assertThat(median(data))
                .columnSourceValues(S1)
                .chars()
                .containsExactly(expected);
    }

    public void check(float expected, float[] data) {
        assertThat(median(data))
                .columnSourceValues(S1)
                .floats()
                .usingElementComparator(Float::compare)
                .containsExactly(expected);
    }

    public void check(double expected, double[] data) {
        assertThat(median(data))
                .columnSourceValues(S1)
                .doubles()
                .usingElementComparator(Double::compare)
                .containsExactly(expected);
    }
}
