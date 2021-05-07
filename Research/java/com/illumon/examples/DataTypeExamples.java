package com.illumon.examples;

import com.illumon.iris.db.tables.Table;
import com.illumon.iris.db.tables.utils.TableTools;
import com.illumon.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;

import static com.illumon.iris.db.tables.utils.DBTimeUtils.convertDateTime;
import static com.illumon.iris.db.tables.utils.TableTools.*;

/**
 * Produce a table with a variety of data types that can be convenient for testing.
 *
 * @IncludeAll
 */
public class DataTypeExamples {
    /**
     * Produce a table with many supported data types.
     *
     * The returned table will include all Deephaven primitives, BigDecimal, BigInteger, and DBDateTimes.  At least one
     * value in each row will be null.
     *
     * @return a table with many supported Deephaven data types
     */
    Table exampleTable() {
        return TableTools.newTable(stringCol("StringValues", "Alpha", "Bravo", "Charlie", "Delta", "Echo", null),
                byteCol("ByteValues", (byte)1, (byte)-2, (byte)3, (byte)4, (byte)-5, QueryConstants.NULL_BYTE),
                shortCol("ShortValues", (short)1, (short)-2, (short)3, (short)4, (short)-5, QueryConstants.NULL_SHORT),
                intCol("IntValues", 1, -2, 3, 4, -5, QueryConstants.NULL_INT),
                longCol("LongValues", 1L, -2L, 3L, 4L, -5L, QueryConstants.NULL_LONG),
                floatCol("FloatValues", 1.1f, -2.2f, Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, QueryConstants.NULL_FLOAT),
                doubleCol("DoubleValues", 1.1, -2.2, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, QueryConstants.NULL_DOUBLE),
                charCol("CharValues", 'A', 'B', 'C', 'D', 'E', QueryConstants.NULL_CHAR),
                col("BigDecimalValues", BigDecimal.valueOf(1.1), BigDecimal.valueOf(2.2), BigDecimal.valueOf(3.3), BigDecimal.valueOf(4.4), BigDecimal.valueOf(5.5), null),
                col("BigIntegerValues", BigInteger.valueOf(1), BigInteger.valueOf(2), BigInteger.valueOf(3), BigInteger.valueOf(4), BigInteger.valueOf(5), null),
                col("DateTimeValues", convertDateTime("2020-04-01T09:30:00 NY"), convertDateTime("2020-04-01T10:30:00 NY"), convertDateTime("2020-04-01T11:30:00 NY"), convertDateTime("2020-04-01T12:30:00 NY"), convertDateTime("2020-04-01T13:30:00 NY"), null)
                );
    }
}
