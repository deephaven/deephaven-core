package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.util.TableTools;

import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.*;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;

public class TestBigIntegerColumns {

    private static BigDecimal atScale(final double v, final int scale) {
        return BigDecimal.valueOf(v).setScale(scale, RoundingMode.HALF_UP);
    }

    @Test
    public void testBigIntegerOps() {
        final Table input = TableTools.newTable(
                col("BI", BigInteger.valueOf(1), BigInteger.valueOf(2), BigInteger.valueOf(3)),
                col("BI2", BigInteger.valueOf(0), BigInteger.valueOf(2), BigInteger.valueOf(4)),
                byteCol("Y", (byte) 7, (byte) 8, (byte) 9),
                shortCol("S", (short) 7, (short) 8, (short) 9),
                longCol("L", 4, 5, 6),
                intCol("I", 7, 8, 9),
                byteCol("Y2", (byte) 0, (byte) 2, (byte) 4),
                shortCol("S2", (short) 0, (short) 2, (short) 4),
                intCol("I2", 0, 2, 4),
                doubleCol("D", 10.5, 11.5, 12.5),
                floatCol("F", 13.5f, 14.5f, 15.5f));
        final Table result = input.select(
                "VPLUS = BI+BI2",
                "VMINUS = (BI-BI2)",
                "VMUL = BI*BI2",
                "VDIV = (BI2 == 0) ? null : (BI/BI2)",
                "V1 = BI*BI",
                "V2 = BI+L",
                "V2A = L+BI",
                "V3 = BI*I < D",
                "V3A = I*BI < D",
                "V3Y = BI*Y < D",
                "V3YA = Y*BI < D",
                "V3S = BI*S < D",
                "V3SA = S*BI < D",
                "V4 = F-BI",
                "V5 = L/BI",
                "V6 = (BI == I2)",
                "V6A = (I2 == BI)",
                "V6Y = (BI == Y2)",
                "V6YA = (Y2 == BI)",
                "V6S = (BI == S2)",
                "V6SA = (S2 == BI)",
                "V6B = (BI == BI2)",
                "V7 = (BI < I2)",
                "V7A = (I2 < BI)",
                "V7B = (BI2 < BI)",
                "V8 = (BI <= I2)",
                "V8A = (I2 <= BI)",
                "V8B = (BI2 <= BI)",
                "V9 = (BI > I2)",
                "V9A = (I2 > BI)",
                "V9B = (BI2 > BI)",
                "V10 = (BI >= I2)",
                "V10A = (I2 >= BI)",
                "V10B = (BI2 >= BI)");

        final int divScale = QueryLanguageFunctionUtils.DEFAULT_SCALE;
        final Table expected = TableTools.newTable(
                col("VPLUS", BigInteger.valueOf(1), BigInteger.valueOf(4), BigInteger.valueOf(7)),
                col("VMINUS", BigInteger.valueOf(1), BigInteger.valueOf(0), BigInteger.valueOf(-1)),
                col("VMUL", BigInteger.valueOf(0), BigInteger.valueOf(4), BigInteger.valueOf(12)),
                col("VDIV", null, atScale(1, divScale), atScale(0.75, divScale)),
                col("V1", BigInteger.valueOf(1), BigInteger.valueOf(4), BigInteger.valueOf(9)),
                col("V2", BigInteger.valueOf(5), BigInteger.valueOf(7), BigInteger.valueOf(9)),
                col("V2A", BigInteger.valueOf(5), BigInteger.valueOf(7), BigInteger.valueOf(9)),
                col("V3", true, false, false),
                col("V3A", true, false, false),
                col("V3Y", true, false, false),
                col("V3YA", true, false, false),
                col("V3S", true, false, false),
                col("V3SA", true, false, false),
                col("V4", BigDecimal.valueOf(12.5), BigDecimal.valueOf(12.5), BigDecimal.valueOf(12.5)),
                col("V5", atScale(4, divScale), atScale(2.5, divScale), atScale(2, divScale)),
                col("V6", false, true, false),
                col("V6A", false, true, false),
                col("V6Y", false, true, false),
                col("V6YA", false, true, false),
                col("V6S", false, true, false),
                col("V6SA", false, true, false),
                col("V6B", false, true, false),
                col("V7", false, false, true),
                col("V7A", true, false, false),
                col("V7B", true, false, false),
                col("V8", false, true, true),
                col("V8A", true, true, false),
                col("V8B", true, true, false),
                col("V9", true, false, false),
                col("V9A", false, false, true),
                col("V9B", false, false, true),
                col("V10", true, true, false),
                col("V10A", false, true, true),
                col("V10B", false, true, true));
        assertTableEquals(expected, result);
    }
}
