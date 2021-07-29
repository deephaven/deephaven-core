/*
 * Copyright (c) 2016-2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.kafka.ingest;

import io.deephaven.configuration.Configuration;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.LiveQueryTable;
import io.deephaven.db.v2.utils.DynamicTableWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;

public class TestGenericRecordAdapter {
    @BeforeClass
    static public void setup() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
    }

    @Before
    public void reset() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @NotNull
    private File getSchemaFile(String name) {
        final String avscPath = Configuration.getInstance().getDevRootPath() + "/KafkaIngester/src/test/resources/avro-examples/";
        return new File(avscPath + name);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testSimple() throws IOException {
        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .buildFactory();

        final Schema avroSchema = new Schema.Parser().parse(getSchemaFile("pageviews.avc"));

        final String [] names = new String[]{"viewtime", "userid", "pageid"};
        final Class [] types = new Class[]{long.class, String.class, String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", 1234L);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final ConsumerRecordToTableWriterAdapter consumer = factory.apply(writer);
        final ConsumerRecord<Object, GenericRecord> record = new ConsumerRecord<>("topic", 0, 0, null, genericRecord);
        consumer.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(longCol("viewtime", 1234L), col("userid", "chuck"), col("pageid", "mcgill"));
        assertTableEquals(expected, result);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testStringKeyIgnored() throws IOException {
        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .buildFactory();

        final Schema avroSchema = new Schema.Parser().parse(getSchemaFile("pageviews.avc"));

        final String [] names = new String[]{"viewtime", "userid", "pageid"};
        final Class [] types = new Class[]{long.class, String.class, String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", 1234L);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final ConsumerRecordToTableWriterAdapter consumer = factory.apply(writer);
        final ConsumerRecord<Object, GenericRecord> record = new ConsumerRecord<>("topic", 0, 0, "KeyString", genericRecord);
        consumer.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(longCol("viewtime", 1234L), col("userid", "chuck"), col("pageid", "mcgill"));
        assertTableEquals(expected, result);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testStringKey() throws IOException {
        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .addColumnToKey("key")
                .buildFactory();

        final Schema avroSchema = new Schema.Parser().parse(getSchemaFile("pageviews.avc"));

        final String [] names = new String[]{"viewtime", "userid", "pageid", "key"};
        final Class [] types = new Class[]{long.class, String.class, String.class, String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", 1234L);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final ConsumerRecordToTableWriterAdapter consumer = factory.apply(writer);
        final ConsumerRecord<Object, GenericRecord> record = new ConsumerRecord<>("topic", 0, 0, "KeyString", genericRecord);
        consumer.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(longCol("viewtime", 1234L), col("userid", "chuck"), col("pageid", "mcgill"), col("key", "KeyString"));
        assertTableEquals(expected, result);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testLongKey() throws IOException {
        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .addColumnToKey("key")
                .buildFactory();

        final Schema avroSchema = new Schema.Parser().parse(getSchemaFile("pageviews.avc"));

        final String [] names = new String[]{"viewtime", "userid", "pageid", "key"};
        final Class [] types = new Class[]{long.class, String.class, String.class, Long.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", 1234L);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final ConsumerRecordToTableWriterAdapter consumer = factory.apply(writer);
        final ConsumerRecord<Object, GenericRecord> record = new ConsumerRecord<>("topic", 0, 0, 42L, genericRecord);
        consumer.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(longCol("viewtime", 1234L), col("userid", "chuck"), col("pageid", "mcgill"), longCol("key", 42));
        assertTableEquals(expected, result);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testSimpleFunction() throws IOException {
        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .addColumnToValueFunction("name", (genericRecord -> genericRecord.get("userid") + " " + genericRecord.get("pageid")))
                .buildFactory();

        final Schema avroSchema = new Schema.Parser().parse(getSchemaFile("pageviews.avc"));

        final String [] names = new String[]{"viewtime", "name"};
        final Class [] types = new Class[]{long.class, String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", 1234L);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final ConsumerRecordToTableWriterAdapter consumer = factory.apply(writer);
        final ConsumerRecord<Object, GenericRecord> record = new ConsumerRecord<>("topic", 0, 0, null, genericRecord);
        consumer.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(longCol("viewtime", 1234L), col("name", "chuck mcgill"));
        assertTableEquals(expected, result);
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testDateTimeFunction() throws IOException {
        final DBDateTime timePoint = DBTimeUtils.convertDateTime("2020-10-09T15:00:00 NY");
        final long nanoEpoch = timePoint.getNanos();
        final long milliEpoch = timePoint.getMillis();

        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .addColumnToValueFunction("viewtime", (genericRecord -> {
                    final Long viewtime = (Long) genericRecord.get("viewtime");
                    if (viewtime == null) {
                        return null;
                    }
                    return new DBDateTime(viewtime);
                }))
                .buildFactory();

        final Schema avroSchema = new Schema.Parser().parse(getSchemaFile("pageviews.avc"));

        final String [] names = new String[]{"viewtime", "userid", "pageid"};
        final Class [] types = new Class[]{DBDateTime.class, String.class, String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", nanoEpoch);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final ConsumerRecordToTableWriterAdapter consumer = factory.apply(writer);
        final ConsumerRecord<Object, GenericRecord> record = new ConsumerRecord<>("topic", 0, 0, null, genericRecord);
        consumer.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(col("viewtime", timePoint), col("userid", "chuck"), col("pageid", "mcgill"));
        assertTableEquals(expected, result);

        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory2 = new GenericRecordConsumerRecordToTableWriterAdapter.Builder()
                .autoValueMapping(true)
                .addColumnToValueFunction("viewtime", (gr) -> {
                    final Long viewtime = (Long) gr.get("viewtime");
                    if (viewtime == null) {
                        return null;
                    }
                    return new DBDateTime(DBTimeUtils.millisToNanos(viewtime));
                })
                .buildFactory();

        final ConsumerRecordToTableWriterAdapter consumer2 = factory2.apply(writer);
        genericRecord.put("viewtime", milliEpoch);
        final ConsumerRecord<Object, GenericRecord> record2 = new ConsumerRecord<>("topic", 0, 0, null, genericRecord);
        consumer2.consumeRecord(record2);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        assertTableEquals(merge(expected, expected), result);
    }
}
