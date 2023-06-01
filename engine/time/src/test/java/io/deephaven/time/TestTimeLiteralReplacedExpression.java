package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

import java.time.*;
import java.util.HashMap;

public class TestTimeLiteralReplacedExpression extends BaseArrayTestCase {

    public void testConvertExpressionDateTime() throws Exception {
        final TimeLiteralReplacedExpression tlre =
                TimeLiteralReplacedExpression.convertExpression("'2010-01-01T12:34:56.891 NY'");
        TestCase.assertEquals("_instant0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_instant0", Instant.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals(
                "        private Instant _instant0=DateTimeUtils.parseInstant(\"2010-01-01T12:34:56.891 NY\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionLocalDate() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'2010-01-01'");
        TestCase.assertEquals("_localDate0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_localDate0", LocalDate.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals(
                "        private java.time.LocalDate _localDate0=DateTimeUtils.parseLocalDate(\"2010-01-01\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionTime() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'PT12:00'");
        TestCase.assertEquals("_nanos0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_nanos0", long.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private long _nanos0=DateTimeUtils.parseDurationNanos(\"PT12:00\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionPeriod() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'P1Y'");
        TestCase.assertEquals("_period0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_period0", Period.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private java.time.Period _period0=DateTimeUtils.parsePeriod(\"P1Y\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionDuration() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'PT1H'");
        TestCase.assertEquals("_duration0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_duration0", Duration.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private java.time.Duration _duration0=DateTimeUtils.parseDuration(\"PT1H\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionLocalTime() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'12:00'");
        TestCase.assertEquals("_localTime0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_localTime0", LocalTime.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals(
                "        private java.time.LocalTime _localTime0=DateTimeUtils.parseLocalTime(\"12:00\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionUnknown() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'g'");
        TestCase.assertEquals("'g'", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("", tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionTimeAddition() throws Exception {
        final TimeLiteralReplacedExpression tlre =
                TimeLiteralReplacedExpression.convertExpression("'PT12:00' + 'PT04:21'");
        TestCase.assertEquals("_nanos0 + _nanos1", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_nanos0", long.class);
        newVars.put("_nanos1", long.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private long _nanos0=DateTimeUtils.parseDurationNanos(\"PT12:00\");\n" +
                "        private long _nanos1=DateTimeUtils.parseDurationNanos(\"PT04:21\");\n",
                tlre.getInstanceVariablesString());
    }

    public void testConvertExpressionTimeAddition2() throws Exception {
        final TimeLiteralReplacedExpression tlre =
                TimeLiteralReplacedExpression.convertExpression("'PT12:00' + 'PT4H'");
        TestCase.assertEquals("_nanos0 + _duration0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_nanos0", long.class);
        newVars.put("_duration0", Duration.class);
        TestCase.assertEquals(newVars, tlre.getNewVariables());

        TestCase.assertEquals("        private long _nanos0=DateTimeUtils.parseDurationNanos(\"PT12:00\");\n" +
                "        private java.time.Duration _duration0=DateTimeUtils.parseDuration(\"PT4H\");\n",
                tlre.getInstanceVariablesString());
    }

}
