//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(OutOfBandTest.class)
public class ParquetStatisticsRoundTripTest {

    private static final String ROOT_FILENAME = ParquetStatisticsRoundTripTest.class.getName() + "_root";
    private File rootFile;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Before
    public void setUp() {
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(rootFile);
    }

    private static Statistics<?> statisticsFor(final File dest, final String columnName) {
        final ParquetMetadata metadata =
                new ParquetTableLocationKey(dest.toURI(), 0, null, ParquetInstructions.EMPTY).getMetadata();
        final MessageType schema = metadata.getFileMetaData().getSchema();
        return metadata.getBlocks().get(0).getColumns()
                .get(schema.getFieldIndex(columnName)).getStatistics();
    }

    /**
     * The handler suites in this folder hand-build {@link Statistics} rather than reading real files, which lets an
     * impossible combination pass unnoticed -- a char column was annotated {@code UINT_8} while holding
     * {@code Character.MAX_VALUE}, a value no {@code UINT_8} column can encode. This test writes each supported type
     * with Deephaven's own writer and pins the encoding those helpers must mirror.
     */
    @Test
    public void writtenStatisticsMatchWhatTheHandlerSuitesAssume() {
        final Table source = TableTools.emptyTable(10).update(
                "byteCol = (byte) ii",
                "charCol = (char) ('a' + ii)",
                "shortCol = (short) ii",
                "intCol = (int) ii",
                "longCol = (long) ii",
                "floatCol = (float) ii",
                "doubleCol = (double) ii",
                "strCol = `s` + ii",
                "instantCol = '2020-01-01T00:00:00Z' + ii * 1000000L");
        final File dest = new File(rootFile, "allTypes.parquet");
        ParquetTools.writeTable(source, dest.getPath());

        assertEncoding("byteCol", dest, INT32, "INTEGER(8,true)");
        // UINT_16, not UINT_8: a char spans 0..65535.
        assertEncoding("charCol", dest, INT32, "INTEGER(16,false)");
        assertEncoding("shortCol", dest, INT32, "INTEGER(16,true)");
        assertEncoding("intCol", dest, INT32, "INTEGER(32,true)");
        assertEncoding("longCol", dest, INT64, null);
        assertEncoding("floatCol", dest, FLOAT, null);
        assertEncoding("doubleCol", dest, DOUBLE, null);
        assertEncoding("strCol", dest, BINARY, "STRING");
        assertEncoding("instantCol", dest, INT64, "TIMESTAMP(NANOS,true)");
    }

    /**
     * Every accessor must actually read the statistics its own writer produces. This is the property the hand-built
     * suites cannot check for themselves.
     */
    @Test
    public void everyAccessorReadsRealWrittenStatistics() {
        final Table source = TableTools.emptyTable(10).update(
                "byteCol = (byte) ii",
                "charCol = (char) ('a' + ii)",
                "shortCol = (short) ii",
                "intCol = (int) ii",
                "longCol = (long) ii",
                "floatCol = (float) ii",
                "doubleCol = (double) ii",
                "strCol = `s` + ii",
                "instantCol = '2020-01-01T00:00:00Z' + ii * 1000000L");
        final File dest = new File(rootFile, "allTypesAccessors.parquet");
        ParquetTools.writeTable(source, dest.getPath());

        assertTrue("byte", MinMaxFromStatistics.getMinMaxForBytes(
                statisticsFor(dest, "byteCol"), v -> {
                }, v -> {
                }));
        assertTrue("char", MinMaxFromStatistics.getMinMaxForChars(
                statisticsFor(dest, "charCol"), v -> {
                }, v -> {
                }));
        assertTrue("short", MinMaxFromStatistics.getMinMaxForShorts(
                statisticsFor(dest, "shortCol"), v -> {
                }, v -> {
                }));
        assertTrue("int", MinMaxFromStatistics.getMinMaxForInts(
                statisticsFor(dest, "intCol"), v -> {
                }, v -> {
                }));
        assertTrue("long", MinMaxFromStatistics.getMinMaxForLongs(
                statisticsFor(dest, "longCol"), v -> {
                }, v -> {
                }));
        assertTrue("float", MinMaxFromStatistics.getMinMaxForFloats(
                statisticsFor(dest, "floatCol"), v -> {
                }, v -> {
                }));
        assertTrue("double", MinMaxFromStatistics.getMinMaxForDoubles(
                statisticsFor(dest, "doubleCol"), v -> {
                }, v -> {
                }));
        assertTrue("string", MinMaxFromStatistics.getMinMaxForStrings(
                statisticsFor(dest, "strCol"), v -> {
                }, v -> {
                }));
        assertTrue("instant", MinMaxFromStatistics.getMinMaxForInstants(
                statisticsFor(dest, "instantCol"), v -> {
                }, v -> {
                }));
    }

    private static void assertEncoding(
            final String column, final File dest,
            final PrimitiveType.PrimitiveTypeName expectedPrimitive, final String expectedLogical) {
        final Statistics<?> stats = statisticsFor(dest, column);
        assertEquals(column + " primitive type", expectedPrimitive, stats.type().getPrimitiveTypeName());
        assertEquals(column + " logical type", expectedLogical,
                String.valueOf(stats.type().getLogicalTypeAnnotation()).equals("null")
                        ? null
                        : String.valueOf(stats.type().getLogicalTypeAnnotation()));
        assertTrue(column + " statistics should be usable", ParquetPushdownUtils.areStatisticsUsable(stats));
        assertTrue(column + " should report its null count", stats.isNumNullsSet());
        assertEquals(column + " has no nulls", 0L, stats.getNumNulls());
    }
}
