/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jdbc;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeFormatter;
import io.deephaven.time.DateTimeFormatters;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.function.ThrowingRunnable;
import org.junit.*;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.*;
import java.util.InputMismatchException;
import java.util.Set;
import java.util.TimeZone;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY;

public class JdbcToTableAdapterTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final ZoneId TZ_UTC = ZoneId.of("UTC");

    private Connection conn;
    private Statement stmt;
    private final long numRows = (long) (2.5 * LARGEST_POOLED_CHUNK_CAPACITY);

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:mem:");
        stmt = conn.createStatement();

        // We need a column with each invalid char: ./\-_
        stmt.executeUpdate("CREATE TABLE TestTable (" +
                "   \"Bool Type\" BIT," +
                "   \"Tiny.Int.Type\" TINYINT," +
                "   \"Small/Int\\Type\" SMALLINT," +
                "   \"Int-Type\" INT," +
                "   \"Big_Int_Type\" BIGINT," +
                "   \"Decimal_Type\" FLOAT," +
                "   \"String Type\" VARCHAR(80)," +
                "   \"DateTime Type\" DATETIME NULL" +
                ");");

        final DateTimeFormatter dtf = DateTimeFormatters.NONISO9.getFormatter();

        for (long ii = 0; ii < numRows; ++ii) {
            stmt.executeUpdate("INSERT INTO TestTable VALUES (" +
                    (ii % 16 == 0 ? "TRUE" : ii % 8 == 0 ? "NULL" : "FALSE") + // boolean
                    ", " + (ii % 256 == 0 ? "NULL" : (ii % 256 - 128)) + // tiny int
                    ", " + (ii % 256 == 1 ? "NULL" : (ii - numRows / 2) & ((1 << 15) - 1)) + // short
                    ", " + (ii % 256 == 2 ? "NULL" : ii - numRows / 2) + // int
                    ", " + (ii % 256 == 3 ? "NULL" : ii - numRows / 2) + // long
                    ", " + (ii % 256 == 4 ? "NULL" : (ii - numRows / 2) / 256.0) + // float
                    ", " + (ii % 256 == 5 ? "NULL" : "'" + ii + "'") + // string
                    ", " + (ii % 256 == 6
                            ? "NULL"
                            : "'" + dtf.format(DateTimeUtils.epochNanosToInstant(ii * 100_000L), TZ_UTC) + "Z'")
                    + // date
                    ");");
        }
    }

    @After
    public void tearDown() throws SQLException {
        conn.close();
    }

    @Test
    public void testEmptyTable() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        final Table result = JdbcToTableAdapter.readJdbc(rs);

        // check no-casing column names
        final Set<String> expectedNames = Set.of("Bool_Type", "TinyIntType", "SmallIntType", "Int_Type",
                "Big_Int_Type", "Decimal_Type", "String_Type", "DateTime_Type");
        Assert.assertEquals(expectedNames, result.getDefinition().getColumnNameSet());

        // should be an empty table
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testLowerCamelCasing() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.lowerCamel, "_");
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // check no-casing column names
        final Set<String> expectedNames = Set.of("boolType", "tinyIntType", "smallIntType", "intType",
                "bigIntType", "decimalType", "stringType", "datetimeType");
        Assert.assertEquals(expectedNames, result.getDefinition().getColumnNameSet());

        // should be an empty table
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testLowercaseCasing() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.lowercase, "_");
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // check no-casing column names
        final Set<String> expectedNames = Set.of("bool_type", "tiny_int_type", "small_int_type", "int_type",
                "big_int_type", "decimal_type", "string_type", "datetime_type");
        Assert.assertEquals(expectedNames, result.getDefinition().getColumnNameSet());

        // should be an empty table
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testUpperCamelCasing() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.UpperCamel, "_");
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // check no-casing column names
        final Set<String> expectedNames = Set.of("BoolType", "TinyIntType", "SmallIntType", "IntType",
                "BigIntType", "DecimalType", "StringType", "DatetimeType");
        Assert.assertEquals(expectedNames, result.getDefinition().getColumnNameSet());

        // should be an empty table
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testUppercaseCasing() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.UPPERCASE, "_");
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // check no-casing column names
        final Set<String> expectedNames = Set.of("BOOL_TYPE", "TINY_INT_TYPE", "SMALL_INT_TYPE", "INT_TYPE",
                "BIG_INT_TYPE", "DECIMAL_TYPE", "STRING_TYPE", "DATETIME_TYPE");
        Assert.assertEquals(expectedNames, result.getDefinition().getColumnNameSet());

        // should be an empty table
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testAlternateReplacement() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.UPPERCASE, "_Z_");
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // check no-casing column names
        final Set<String> expectedNames = Set.of("BOOL_Z_TYPE", "TINY_Z_INT_Z_TYPE", "SMALL_Z_INT_Z_TYPE", "INT_Z_TYPE",
                "BIG_Z_INT_Z_TYPE", "DECIMAL_Z_TYPE", "STRING_Z_TYPE", "DATETIME_Z_TYPE");
        Assert.assertEquals(expectedNames, result.getDefinition().getColumnNameSet());

        // should be an empty table
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testDefaultResultTypes() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.lowercase, "_");
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // should have converted all rows
        Assert.assertEquals(numRows, result.size());

        final ColumnSource<Boolean> bool_type = result.getColumnSource("bool_type");
        final ColumnSource<Short> tiny_int_type = result.getColumnSource("tiny_int_type");
        final ColumnSource<Short> small_int_type = result.getColumnSource("small_int_type");
        final ColumnSource<Integer> int_type = result.getColumnSource("int_type");
        final ColumnSource<Long> big_int_type = result.getColumnSource("big_int_type");
        final ColumnSource<Double> decimal_type = result.getColumnSource("decimal_type");
        final ColumnSource<String> string_type = result.getColumnSource("string_type");
        final ColumnSource<Instant> instant_type = result.getColumnSource("datetime_type");

        // check expected column sources types
        Assert.assertEquals(Boolean.class, bool_type.getType());
        Assert.assertEquals(short.class, tiny_int_type.getType());
        Assert.assertEquals(short.class, small_int_type.getType());
        Assert.assertEquals(int.class, int_type.getType());
        Assert.assertEquals(long.class, big_int_type.getType());
        Assert.assertEquals(double.class, decimal_type.getType());
        Assert.assertEquals(String.class, string_type.getType());
        Assert.assertEquals(Instant.class, instant_type.getType());

        // Check table values
        for (long ii = 0; ii < result.size(); ++ii) {
            if (ii % 16 == 0) {
                Assert.assertTrue(bool_type.get(ii));
            } else if (ii % 8 == 0) {
                Assert.assertNull(bool_type.get(ii));
            } else {
                Assert.assertFalse(bool_type.get(ii));
            }

            if (ii % 256 == 0) {
                Assert.assertEquals(QueryConstants.NULL_SHORT, tiny_int_type.getShort(ii));
            } else {
                Assert.assertEquals((ii % 256) - 128, tiny_int_type.getShort(ii));
            }

            if (ii % 256 == 1) {
                Assert.assertEquals(QueryConstants.NULL_SHORT, small_int_type.getShort(ii));
            } else {
                Assert.assertEquals((ii - numRows / 2) & ((1 << 15) - 1), small_int_type.getShort(ii));
            }

            if (ii % 256 == 2) {
                Assert.assertEquals(QueryConstants.NULL_INT, int_type.getInt(ii));
            } else {
                Assert.assertEquals(ii - numRows / 2, int_type.getInt(ii));
            }

            if (ii % 256 == 3) {
                Assert.assertEquals(QueryConstants.NULL_LONG, big_int_type.getLong(ii));
            } else {
                Assert.assertEquals(ii - numRows / 2, big_int_type.getLong(ii));
            }

            if (ii % 256 == 4) {
                Assert.assertEquals(QueryConstants.NULL_DOUBLE, decimal_type.getDouble(ii), 1e-3);
            } else {
                // noinspection IntegerDivisionInFloatingPointContext
                Assert.assertEquals((ii - numRows / 2) / 256.0, decimal_type.getDouble(ii), 1e-3);
            }

            if (ii % 256 == 5) {
                Assert.assertNull(string_type.get(ii));
            } else {
                Assert.assertEquals(Long.toString(ii), string_type.get(ii));
            }

            if (ii % 256 == 6) {
                Assert.assertNull(instant_type.get(ii));
            } else {
                final Instant dt = instant_type.get(ii);
                // is only accurate
                Assert.assertEquals(ii * 100_000L, DateTimeUtils.epochNanos(dt));
            }
        }
    }

    @Test
    public void testDoubleArrayType() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"ArrayColumn\" VARCHAR(80)" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "('(1.0, 2.0, 3.0)')" +
                ",('{4.0, 5.0, 6.0}')" +
                ",('[7.0, 8.0, 9.0]')" +
                ",NULL" +
                ";");

        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnTargetType("ArrayColumn", double[].class);
        Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<double[]> cs = result.getColumnSource("ArrayColumn");

        Assert.assertEquals(4, result.size());
        Assert.assertArrayEquals(new double[] {1.0, 2.0, 3.0}, cs.get(0), 1e-8);
        Assert.assertArrayEquals(new double[] {4.0, 5.0, 6.0}, cs.get(1), 1e-8);
        Assert.assertArrayEquals(new double[] {7.0, 8.0, 9.0}, cs.get(2), 1e-8);
        Assert.assertNull(cs.get(3));
    }

    @Test
    public void testLongArrayType() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"ArrayColumn\" VARCHAR(80)" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "('(1, 2, 3)')" +
                ",('{4, 5, 6}')" +
                ",('[7, 8, 9]')" +
                ",NULL" +
                ";");

        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnTargetType("ArrayColumn", long[].class);
        final Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        final ColumnSource<long[]> cs = result.getColumnSource("ArrayColumn");

        Assert.assertEquals(4, result.size());
        Assert.assertArrayEquals(new long[] {1, 2, 3}, cs.get(0));
        Assert.assertArrayEquals(new long[] {4, 5, 6}, cs.get(1));
        Assert.assertArrayEquals(new long[] {7, 8, 9}, cs.get(2));
        Assert.assertNull(cs.get(3));
    }

    private void testArrayParseStrictHelper(
            final ThrowingRunnable<SQLException> insertRow,
            final double[] expectedRow) throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"ArrayColumn\" VARCHAR(80)" +
                ");");

        insertRow.run();

        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnTargetType("ArrayColumn", double[].class);

        try {
            JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
            Assert.fail("Did not throw Input");
        } catch (InputMismatchException ignored) {
        }

        options.strict(false);
        final Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<double[]> cs = result.getColumnSource("ArrayColumn");
        Assert.assertEquals(1, result.size());
        Assert.assertArrayEquals(expectedRow, cs.get(0), 1e-8);
    }

    @Test
    public void testArrayParserNoBraces() throws SQLException {
        testArrayParseStrictHelper(() -> stmt.executeUpdate("INSERT INTO SingleTestTable VALUES (" +
                "'1.0, 2.0, 3.0'" +
                ");"), new double[] {1.0, 2.0, 3.0});
    }

    @Test
    public void testArrayParserMismatchBraces() throws SQLException {
        testArrayParseStrictHelper(() -> stmt.executeUpdate("INSERT INTO SingleTestTable VALUES (" +
                "'{1.0, 2.0, 3.0)'" +
                ");"), new double[] {1.0, 2.0, 3.0});
    }

    @Test
    public void testArrayParserMissingOpen() throws SQLException {
        testArrayParseStrictHelper(() -> stmt.executeUpdate("INSERT INTO SingleTestTable VALUES (" +
                "'1.0, 2.0, 3.0)'" +
                ");"), new double[] {1.0, 2.0, 3.0});
    }

    @Test
    public void testArrayParserMissingClose() throws SQLException {
        testArrayParseStrictHelper(() -> stmt.executeUpdate("INSERT INTO SingleTestTable VALUES (" +
                "'[1.0, 2.0, 3.0'" +
                ");"), new double[] {1.0, 2.0, 3.0});
    }

    @Test
    public void testArrayParserShort() throws SQLException {
        testArrayParseStrictHelper(() -> stmt.executeUpdate("INSERT INTO SingleTestTable VALUES (" +
                "'1'" +
                ");"), new double[] {1.0});
    }

    @Test
    public void testArrayDelimiterOption() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"ArrayColumn\" VARCHAR(80)" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "('(1.0# 2.0# 3.0)')" +
                ",('{4.0# 5.0# 6.0}')" +
                ",('[7.0# 8.0# 9.0]')" +
                ";");

        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnTargetType("ArrayColumn", double[].class);
        options.arrayDelimiter("#");
        final Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        final ColumnSource<double[]> cs = result.getColumnSource("ArrayColumn");

        Assert.assertEquals(3, result.size());
        TableTools.show(result);
        Assert.assertArrayEquals(new double[] {1.0, 2.0, 3.0}, cs.get(0), 1e-8);
        Assert.assertArrayEquals(new double[] {4.0, 5.0, 6.0}, cs.get(1), 1e-8);
        Assert.assertArrayEquals(new double[] {7.0, 8.0, 9.0}, cs.get(2), 1e-8);
    }

    @Test
    public void testMaxRowsOption() throws SQLException {
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable");
        final JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.maxRows(32);
        final Table result = JdbcToTableAdapter.readJdbc(rs, options);

        // should be only the first 32 rows
        Assert.assertEquals(32, result.size());
    }

    @Test
    public void testScrollableResultSet() throws SQLException {
        stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable");
        Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());

        // scroll sensitive/insensitive uses an alternative column source type
        final Table result = JdbcToTableAdapter.readJdbc(rs);
        Assert.assertEquals(numRows, result.size());
    }

    @Test
    public void testScrollableEmptyResultSet() throws SQLException {
        stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        final ResultSet rs = stmt.executeQuery("SELECT * from TestTable LIMIT 0");
        Assert.assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());

        // scroll sensitive/insensitive uses an alternative column source type
        final Table result = JdbcToTableAdapter.readJdbc(rs);
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testByteArrayColumn() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"BlobColumn\" BLOB" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "'Hello World'" +
                ",NULL" +
                ";");

        final Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"));
        final ColumnSource<byte[]> cs = result.getColumnSource("BlobColumn");

        Assert.assertEquals(2, result.size());
        Assert.assertArrayEquals("Hello World".getBytes(), cs.get(0));
        Assert.assertNull(cs.get(1));
    }


    @Test
    public void testDecimalTypeMapping() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"FloatCol\" REAL" +
                ",  \"DoubleCol\" FLOAT" +
                ",  \"DecimalCol\" DECIMAL(7, 3)" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "(1567.89, 2567.89, 3567.89)" +
                ",(NULL, NULL, NULL)" +
                ";");

        Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"));
        ColumnSource<Float> floatCS = result.getColumnSource("FloatCol");
        ColumnSource<BigDecimal> bigdecimalCS = result.getColumnSource("DecimalCol");
        ColumnSource<Double> doubleCS = result.getColumnSource("DoubleCol");

        // Check default mappings:
        Assert.assertEquals(2, result.size());
        Assert.assertEquals(1567.89, floatCS.getFloat(0), 1e-3);
        Assert.assertEquals(2567.89, doubleCS.getDouble(0), 1e-3);
        Assert.assertEquals(3567.89, bigdecimalCS.get(0).doubleValue(), 1e-3);

        // Check null values:
        Assert.assertEquals(QueryConstants.NULL_FLOAT, floatCS.getFloat(1), 1e-3);
        Assert.assertEquals(QueryConstants.NULL_DOUBLE, doubleCS.getDouble(1), 1e-3);
        Assert.assertNull(bigdecimalCS.get(1));

        // Check float to BigDecimal:
        JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnTargetType("FloatCol", BigDecimal.class);
        result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT \"FloatCol\" FROM SingleTestTable"), options);
        bigdecimalCS = result.getColumnSource("FloatCol");
        Assert.assertEquals(1567.89, bigdecimalCS.get(0).doubleValue(), 1e-3);
        Assert.assertNull(bigdecimalCS.get(1));

        // Check double to BigDecimal:
        options.columnTargetType("DoubleCol", BigDecimal.class);
        result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT \"DoubleCol\" FROM SingleTestTable"), options);
        bigdecimalCS = result.getColumnSource("DoubleCol");
        Assert.assertEquals(2567.89, bigdecimalCS.get(0).doubleValue(), 1e-3);
        Assert.assertNull(bigdecimalCS.get(1));
        TableTools.show(result);

        // Check BigDecimal to float:
        options.columnTargetType("DecimalCol", float.class);
        result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT \"DecimalCol\" FROM SingleTestTable"), options);
        floatCS = result.getColumnSource("DecimalCol");
        Assert.assertEquals(3567.89, floatCS.getFloat(0), 1e-3);
        Assert.assertEquals(QueryConstants.NULL_FLOAT, floatCS.getFloat(1), 1e-3);

        // Check BigDecimal to double:
        options.columnTargetType("DecimalCol", double.class);
        result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT \"DecimalCol\" FROM SingleTestTable"), options);
        doubleCS = result.getColumnSource("DecimalCol");
        Assert.assertEquals(3567.89, doubleCS.getDouble(0), 1e-3);
        Assert.assertEquals(QueryConstants.NULL_DOUBLE, doubleCS.getDouble(1), 1e-3);
    }

    @Test
    public void testBoxedTypeMapsToPrimitive() throws SQLException {
        JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.lowerCamel, "_");
        options.columnTargetType("smallIntType", Short.class);
        options.columnTargetType("intType", Integer.class);
        options.columnTargetType("bigIntType", Long.class);
        options.columnTargetType("decimalType", Double.class);

        Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM TestTable"), options);
        Assert.assertEquals(short.class, result.getColumnSource("smallIntType").getType());
        Assert.assertEquals(int.class, result.getColumnSource("intType").getType());
        Assert.assertEquals(long.class, result.getColumnSource("bigIntType").getType());
        Assert.assertEquals(double.class, result.getColumnSource("decimalType").getType());
    }

    @Test
    public void testCharTypeMapping() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"CharCol\" CHAR(1)" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "'H'" +
                ",NULL" +
                ";");

        JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnTargetType("CharCol", char.class);
        Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<Character> cs = result.getColumnSource("CharCol");
        Assert.assertEquals('H', cs.getChar(0));
        Assert.assertEquals(QueryConstants.NULL_CHAR, cs.getChar(1));
    }

    @Test
    public void testDateTypeMapping() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"DateCol\" DATE" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "'2022-02-22'" +
                ",NULL" +
                ";");

        final LocalDate expectedDate = LocalDate.of(2022, 2, 22);

        // Test for default mapping to java LocalDate
        JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<LocalDate> ldcs = result.getColumnSource("DateCol");
        Assert.assertEquals(expectedDate, ldcs.get(0));
        Assert.assertNull(ldcs.get(1));

        // Convert to Instant
        options.columnTargetType("DateCol", Instant.class);
        options.sourceTimeZone(TimeZone.getTimeZone("UTC"));
        result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<Instant> dtcs = result.getColumnSource("DateCol");

        final long epochTm = expectedDate.atStartOfDay().toEpochSecond(ZoneOffset.UTC);
        Assert.assertEquals(epochTm, dtcs.get(0).toEpochMilli() / 1000);
        Assert.assertNull(dtcs.get(1));
    }

    @Test
    public void testLocalTimeTypeMapping() throws SQLException {
        stmt.executeUpdate("DROP TABLE IF EXISTS SingleTestTable");
        stmt.executeUpdate("CREATE TABLE SingleTestTable (" +
                "   \"TimeCol\" TIME(2)" +
                ");");

        stmt.executeUpdate("INSERT INTO SingleTestTable VALUES " +
                "'10:22:10.22'" +
                ",NULL" +
                ";");

        final LocalTime expectedTime = LocalTime.of(10, 22, 10, 220 * 1000000);

        // Test for default mapping to java LocalTime
        JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        Table result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<LocalTime> ltcs = result.getColumnSource("TimeCol");
        Assert.assertEquals(expectedTime, ltcs.get(0));
        Assert.assertNull(ltcs.get(1));

        // Convert to Long
        options.columnTargetType("TimeCol", long.class);
        options.sourceTimeZone(TimeZone.getTimeZone("UTC"));
        result = JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM SingleTestTable"), options);
        ColumnSource<Long> dtcs = result.getColumnSource("TimeCol");

        Assert.assertEquals(expectedTime.toNanoOfDay(), dtcs.getLong(0));
        Assert.assertEquals(QueryConstants.NULL_LONG, dtcs.getLong(1));
    }

    @Test
    public void testThrowsMapperException() throws SQLException {
        JdbcToTableAdapter.ReadJdbcOptions options = JdbcToTableAdapter.readJdbcOptions();
        options.columnNameFormat(JdbcToTableAdapter.CasingStyle.lowerCamel, "_");
        options.columnTargetType("smallIntType", Object.class);

        // let's just make sure we get the expected mapping exception
        try {
            JdbcToTableAdapter.readJdbc(stmt.executeQuery("SELECT * FROM TestTable"), options);
            Assert.fail("Did not throw expected JdbcTypeMapperException");
        } catch (final JdbcTypeMapperException ignored) {
        }
    }
}
