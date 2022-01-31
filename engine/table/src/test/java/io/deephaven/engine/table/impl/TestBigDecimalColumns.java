package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.util.TableTools;

import static io.deephaven.engine.table.impl.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.*;

import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class TestBigDecimalColumns {

    private static BigDecimal atScale(final double v, final int scale) {
        return BigDecimal.valueOf(v).setScale(scale, RoundingMode.HALF_UP);
    }

    @Test
    public void testBigDecimalOps() {
        final Table input = TableTools.newTable(
                col("BD", BigDecimal.valueOf(1), BigDecimal.valueOf(2), BigDecimal.valueOf(3)),
                col("BD2", BigDecimal.valueOf(0), BigDecimal.valueOf(2), BigDecimal.valueOf(4)),
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
                "VPLUS = BD+BD2",
                "VMINUS = (BD-BD2)",
                "VMUL = BD*BD2",
                "VDIV = (BD2 == 0) ? null : (BD/BD2)",
                "V1 = BD*BD",
                "V2 = BD+L",
                "V2A = L+BD",
                "V3 = BD*I < D",
                "V3A = I*BD < D",
                "V3Y = BD*Y < D",
                "V3YA = Y*BD < D",
                "V3S = BD*S < D",
                "V3SA = S*BD < D",
                "V4 = F-BD",
                "V5 = L/BD",
                "V6 = (BD == I2)",
                "V6A = (I2 == BD)",
                "V6Y = (BD == Y2)",
                "V6YA = (Y2 == BD)",
                "V6S = (BD == S2)",
                "V6SA = (S2 == BD)",
                "V6B = (BD == BD2)",
                "V7 = (BD < I2)",
                "V7A = (I2 < BD)",
                "V7B = (BD2 < BD)",
                "V8 = (BD <= I2)",
                "V8A = (I2 <= BD)",
                "V8B = (BD2 <= BD)",
                "V9 = (BD > I2)",
                "V9A = (I2 > BD)",
                "V9B = (BD2 > BD)",
                "V10 = (BD >= I2)",
                "V10A = (I2 >= BD)",
                "V10B = (BD2 >= BD)");
        final int divScale = QueryLanguageFunctionUtils.DEFAULT_SCALE;
        final Table expected = TableTools.newTable(
                col("VPLUS", BigDecimal.valueOf(1), BigDecimal.valueOf(4), BigDecimal.valueOf(7)),
                col("VMINUS", BigDecimal.valueOf(1), BigDecimal.valueOf(0), BigDecimal.valueOf(-1)),
                col("VMUL", BigDecimal.valueOf(0), BigDecimal.valueOf(4), BigDecimal.valueOf(12)),
                col("VDIV", null, atScale(1, divScale), atScale(0.75, divScale)),
                col("V1", BigDecimal.valueOf(1), BigDecimal.valueOf(4), BigDecimal.valueOf(9)),
                col("V2", BigDecimal.valueOf(5), BigDecimal.valueOf(7), BigDecimal.valueOf(9)),
                col("V2A", BigDecimal.valueOf(5), BigDecimal.valueOf(7), BigDecimal.valueOf(9)),
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
