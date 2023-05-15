package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

import java.time.LocalDate;
import java.util.HashMap;

public class TestTimeLiteralReplacedExpression extends BaseArrayTestCase {

    public void testFailDateTimeUtils() {
        fail("Check date time utils function names");
    }

    public void testFail() {
        TestCase.fail("Needs many more tests after DateTimeUtils has been tested.");
    }

    public void testConvertExpressionDateTime() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'2010-01-01T12:34:567.891 NY'");
        TestCase.assertEquals("_dateTime0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_dateTime0", String.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private DateTime _dateTime0=DateTimeUtils.parseDateTime(\"2010-01-01T12:34:567.891 NY\");\n", tlre.getInstanceVariablesString());

    }

    public void testConvertExpressionLocalDate() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'2010-01-01'");
        TestCase.assertEquals("_localDate0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_localDate0", LocalDate.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private java.time.LocalDate _localDate0=DateTimeUtils.parseLocalDate(\"2010-01-01\");\n", tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionTime() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'12:00'");
        TestCase.assertEquals("_time0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_time0", long.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private long _time0=DateTimeUtils.convertTime(\"12:00\");\n", tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionPeriod() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'T1S'");
        TestCase.assertEquals("_period0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_period0", Period.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private Period _period0=DateTimeUtils.convertPeriod(\"T1S\");\n", tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionUnknown() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'g'");
        TestCase.assertEquals("'g'", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("", tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionTimeAddition() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'12:00' + '04:21'");
        TestCase.assertEquals("_time0 + _time1", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_time0", long.class);
        newVars.put("_time1", long.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private long _time0=DateTimeUtils.convertTime(\"12:00\");\n" +
                "        private long _time1=DateTimeUtils.convertTime(\"04:21\");\n", tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionTimeAddition2() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'12:00' + 'T4H'");
        TestCase.assertEquals("_time0 + _period0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_time0", long.class);
        newVars.put("_period0", Period.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private long _time0=DateTimeUtils.convertTime(\"12:00\");\n" +
                "        private Period _period0=DateTimeUtils.convertPeriod(\"T4H\");\n", tlre.getInstanceVariablesString());
    }

}
