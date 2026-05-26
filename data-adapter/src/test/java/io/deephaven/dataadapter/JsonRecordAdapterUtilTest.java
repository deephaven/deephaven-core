//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter;

import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.deephaven.dataadapter.rec.json.JsonRecordAdapterUtil;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.Arrays;

public class JsonRecordAdapterUtilTest extends RefreshingTableTestCase {

    private static final NullNode NULL_JSON_NODE = NullNode.getInstance();

    public void testJsonRecordAdapterUtilAllPrimitiveAndSpecialTypes() throws Exception {
        final Instant instant = Instant.parse("2020-01-02T03:04:05Z");
        final LocalDate localDate = LocalDate.of(2020, 1, 2);
        final LocalTime localTime = LocalTime.of(3, 4, 5);
        final LocalDateTime localDateTime = LocalDateTime.of(2020, 1, 2, 3, 4, 5);
        final Duration duration = Duration.ofSeconds(123);
        final Period period = Period.ofDays(45);
        final BigInteger bigInteger = new BigInteger("12345678901234567890");
        final BigDecimal bigDecimal = new BigDecimal("12345.6789");
        final byte[] bytes = new byte[] {1, 2, 3};

        final Table source = TableTools.newTable(
                TableTools.byteCol("ByteCol", (byte) 1, QueryConstants.NULL_BYTE),
                TableTools.shortCol("ShortCol", (short) 2, QueryConstants.NULL_SHORT),
                TableTools.intCol("IntCol", 3, QueryConstants.NULL_INT),
                TableTools.longCol("LongCol", 4L, QueryConstants.NULL_LONG),
                TableTools.floatCol("FloatCol", 1.5f, QueryConstants.NULL_FLOAT),
                TableTools.doubleCol("DoubleCol", 2.5d, QueryConstants.NULL_DOUBLE),
                TableTools.charCol("CharCol", 'A', QueryConstants.NULL_CHAR),
                TableTools.booleanCol("BooleanCol", true, null),
                TableTools.col("StringCol", "Alpha", null),
                TableTools.col("CharSequenceCol", new StringBuilder("Beta"), null),
                TableTools.instantCol("InstantCol", instant, null),
                TableTools.col("LocalDateCol", localDate, null),
                TableTools.col("LocalTimeCol", localTime, null),
                TableTools.col("LocalDateTimeCol", localDateTime, null),
                TableTools.col("DurationCol", duration, null),
                TableTools.col("PeriodCol", period, null),
                TableTools.col("BigIntegerCol", bigInteger, null),
                TableTools.col("BigDecimalCol", bigDecimal, null),
                TableTools.col("ByteArrayCol", bytes, null));

        final TableToRecordAdapter<ObjectNode> recordAdapter = new TableToRecordAdapter<>(
                source,
                JsonRecordAdapterUtil.createJsonRecordAdapterDescriptor(
                        source.getDefinition(),
                        Arrays.asList(
                                "ByteCol",
                                "ShortCol",
                                "IntCol",
                                "LongCol",
                                "FloatCol",
                                "DoubleCol",
                                "CharCol",
                                "BooleanCol",
                                "StringCol",
                                "CharSequenceCol",
                                "InstantCol",
                                "LocalDateCol",
                                "LocalTimeCol",
                                "LocalDateTimeCol",
                                "DurationCol",
                                "PeriodCol",
                                "BigIntegerCol",
                                "BigDecimalCol",
                                "ByteArrayCol")));

        final ObjectNode[] records = recordAdapter.getRecords();
        assertEquals(2, records.length);

        final ObjectNode record = records[0];
        assertEquals((byte) 1, (byte) record.get("ByteCol").shortValue());
        assertEquals((short) 2, record.get("ShortCol").shortValue());
        assertEquals(3, record.get("IntCol").intValue());
        assertEquals(4L, record.get("LongCol").longValue());
        assertEquals(1.5f, record.get("FloatCol").floatValue());
        assertEquals(2.5d, record.get("DoubleCol").doubleValue());
        assertEquals("A", record.get("CharCol").textValue());
        assertEquals(true, record.get("BooleanCol").booleanValue());
        assertEquals("Alpha", record.get("StringCol").textValue());
        assertEquals("Beta", record.get("CharSequenceCol").textValue());
        assertEquals(instant.toString(), record.get("InstantCol").textValue());
        assertEquals(localDate.toString(), record.get("LocalDateCol").textValue());
        assertEquals(localTime.toString(), record.get("LocalTimeCol").textValue());
        assertEquals(localDateTime.toString(), record.get("LocalDateTimeCol").textValue());
        assertEquals(duration.toString(), record.get("DurationCol").textValue());
        assertEquals(period.toString(), record.get("PeriodCol").textValue());
        assertEquals(bigInteger, record.get("BigIntegerCol").bigIntegerValue());
        assertEquals(bigDecimal, record.get("BigDecimalCol").decimalValue());
        assertTrue(Arrays.equals(bytes, record.get("ByteArrayCol").binaryValue()));

        final ObjectNode nullRecord = records[1];
        assertEquals(NULL_JSON_NODE, nullRecord.get("ByteCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("ShortCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("IntCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("LongCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("FloatCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("DoubleCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("CharCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("BooleanCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("StringCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("CharSequenceCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("InstantCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("LocalDateCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("LocalTimeCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("LocalDateTimeCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("DurationCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("PeriodCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("BigIntegerCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("BigDecimalCol"));
        assertEquals(NULL_JSON_NODE, nullRecord.get("ByteArrayCol"));
    }
}
