//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import org.junit.Test;

import java.time.*;
import java.util.HashMap;

import static org.junit.Assert.*;

public class TestTimeLiteralReplacedExpression {

    @Test
    public void testConvertExpressionDateTime() throws Exception {
        final TimeLiteralReplacedExpression tlre =
                TimeLiteralReplacedExpression.convertExpression("'2010-01-01T12:34:56.891 NY'");
        assertEquals("_instant0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_instant0", Instant.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private Instant _instant0=DateTimeUtils.parseInstant(\"2010-01-01T12:34:56.891 NY\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionLocalDate() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'2010-01-01'");
        assertEquals("_localDate0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_localDate0", LocalDate.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.LocalDate _localDate0=DateTimeUtils.parseLocalDate(\"2010-01-01\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionTime() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'PT12:00'");
        assertEquals("_duration0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_duration0", Duration.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.Duration _duration0=DateTimeUtils.parseDuration(\"PT12:00\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionPeriod() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'P1Y'");
        assertEquals("_period0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_period0", Period.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals("        private java.time.Period _period0=DateTimeUtils.parsePeriod(\"P1Y\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionDuration() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'PT1H'");
        assertEquals("_duration0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_duration0", Duration.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals("        private java.time.Duration _duration0=DateTimeUtils.parseDuration(\"PT1H\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionLocalTime() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'12:00'");
        assertEquals("_localTime0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_localTime0", LocalTime.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.LocalTime _localTime0=DateTimeUtils.parseLocalTime(\"12:00\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionTimeZone() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'America/Denver'");
        assertEquals("_timeZone0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_timeZone0", ZoneId.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.ZoneId _timeZone0=DateTimeUtils.parseTimeZone(\"America/Denver\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionTimeZone2() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'NY'");
        assertEquals("_timeZone0", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_timeZone0", ZoneId.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.ZoneId _timeZone0=DateTimeUtils.parseTimeZone(\"NY\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionUnknown() throws Exception {
        final TimeLiteralReplacedExpression tlre = TimeLiteralReplacedExpression.convertExpression("'g'");
        assertEquals("'g'", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals("", tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionTimeAddition() throws Exception {
        final TimeLiteralReplacedExpression tlre =
                TimeLiteralReplacedExpression.convertExpression("'PT12:00' + 'PT04:21'");
        assertEquals("_duration0 + _duration1", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_duration0", Duration.class);
        newVars.put("_duration1", Duration.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.Duration _duration0=DateTimeUtils.parseDuration(\"PT12:00\");\n" +
                        "        private java.time.Duration _duration1=DateTimeUtils.parseDuration(\"PT04:21\");\n",
                tlre.getInstanceVariablesString());
    }

    @Test
    public void testConvertExpressionTimeAddition2() throws Exception {
        final TimeLiteralReplacedExpression tlre =
                TimeLiteralReplacedExpression.convertExpression("'PT12:00' + 'PT4H'");
        assertEquals("_duration0 + _duration1", tlre.getConvertedFormula());

        final HashMap<String, Class<?>> newVars = new HashMap<>();
        newVars.put("_duration0", Duration.class);
        newVars.put("_duration1", Duration.class);
        assertEquals(newVars, tlre.getNewVariables());

        assertEquals(
                "        private java.time.Duration _duration0=DateTimeUtils.parseDuration(\"PT12:00\");\n" +
                        "        private java.time.Duration _duration1=DateTimeUtils.parseDuration(\"PT4H\");\n",
                tlre.getInstanceVariablesString());
    }

}
