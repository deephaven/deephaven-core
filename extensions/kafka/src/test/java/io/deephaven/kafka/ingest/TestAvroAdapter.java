/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.chunk.*;
import io.deephaven.util.BooleanUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseableArray;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class TestAvroAdapter {
    @NotNull
    private InputStream getSchemaFile(String name) {
        return new BufferedInputStream(TestAvroAdapter.class.getResourceAsStream("/avro-examples/" + name));
    }

    private Schema getSchema(String name) throws IOException {
        try (final InputStream in = getSchemaFile(name)) {
            return new Schema.Parser().parse(in);
        }
    }

    @Test
    public void testSimple() throws IOException {
        final Schema avroSchema = getSchema("pageviews.avc");

        final String[] names = new String[] {"viewtime", "userid", "pageid"};
        final Class<?>[] types = new Class[] {long.class, String.class, String.class};
        final TableDefinition definition = TableDefinition.from(Arrays.asList(names), Arrays.asList(types));

        final GenericData.Record genericRecord = new GenericData.Record(avroSchema);
        genericRecord.put("viewtime", 1234L);
        genericRecord.put("userid", "chuck");
        genericRecord.put("pageid", "mcgill");

        final Map<String, String> colMap = new HashMap<>();
        colMap.put("viewtime", "viewtime");
        colMap.put("userid", "userid");
        colMap.put("pageid", "pageid");

        try (final WritableObjectChunk<Object, Values> inputValues =
                WritableObjectChunk.makeWritableChunk(1)) {
            inputValues.setSize(0);
            inputValues.add(genericRecord);

            final WritableChunk[] output = new WritableChunk[3];
            try (final SafeCloseableArray ignored = new SafeCloseableArray(output)) {
                output[0] = WritableLongChunk.makeWritableChunk(1);
                output[1] = WritableObjectChunk.makeWritableChunk(1);
                output[2] = WritableObjectChunk.makeWritableChunk(1);

                for (WritableChunk wc : output) {
                    wc.setSize(0);
                }

                final GenericRecordChunkAdapter adapter = GenericRecordChunkAdapter.make(definition,
                        (idx) -> output[idx].getChunkType(), colMap, Pattern.compile(Pattern.quote(".")), avroSchema,
                        true);
                adapter.handleChunk(inputValues, output);

                TestCase.assertEquals(1, output[0].size());
                TestCase.assertEquals(1, output[1].size());
                TestCase.assertEquals(1, output[2].size());

                TestCase.assertEquals(1234L, output[0].asLongChunk().get(0));
                TestCase.assertEquals("chuck", output[1].asObjectChunk().get(0));
                TestCase.assertEquals("mcgill", output[2].asObjectChunk().get(0));
            }
        }
    }

    @Test
    public void testTimestamp() throws IOException {
        final Schema avroSchema = getSchema("fieldtest.avsc");

        final String[] names = new String[] {"last_name", "number", "truthiness", "timestamp", "timestampMicros",
                "timeMillis", "timeMicros"};
        final Class<?>[] types = new Class[] {String.class, int.class, boolean.class, Instant.class, Instant.class,
                int.class, long.class};
        final TableDefinition definition = TableDefinition.from(Arrays.asList(names), Arrays.asList(types));

        final Instant dt1 = DateTimeUtils.parseInstant("2021-08-23T12:00:00.123456789 NY");
        final Instant dt2 = DateTimeUtils.parseInstant("2021-08-23T13:00:00.500600700 NY");

        final GenericData.Record genericRecord1 = new GenericData.Record(avroSchema);
        genericRecord1.put("last_name", "LN1");
        genericRecord1.put("number", 32);
        genericRecord1.put("truthiness", false);
        genericRecord1.put("timestamp", dt1.toEpochMilli());
        genericRecord1.put("timestampMicros", DateTimeUtils.epochMicros(dt1));
        genericRecord1.put("timeMillis", 10000);
        genericRecord1.put("timeMicros", 100000L);

        final GenericData.Record genericRecord2 = new GenericData.Record(avroSchema);
        genericRecord2.put("last_name", null);
        genericRecord2.put("number", 64);
        genericRecord2.put("truthiness", true);
        genericRecord2.put("timestamp", dt2.toEpochMilli());
        genericRecord2.put("timestampMicros", DateTimeUtils.epochMicros(dt2));
        genericRecord2.put("timeMillis", 20000);
        genericRecord2.put("timeMicros", 200000L);

        final GenericData.Record genericRecord3 = new GenericData.Record(avroSchema);
        genericRecord3.put("last_name", "LN3");
        genericRecord3.put("number", 128);
        genericRecord3.put("truthiness", null);
        genericRecord3.put("timestamp", null);
        genericRecord3.put("timestampMicros", null);
        genericRecord3.put("timeMillis", 30000);
        genericRecord3.put("timeMicros", 300000L);

        final Map<String, String> colMap = new HashMap<>();
        for (String s : new String[] {"last_name", "number", "truthiness", "timestamp", "timestampMicros", "timeMillis",
                "timeMicros"}) {
            colMap.put(s, s);
        }

        try (final WritableObjectChunk<Object, Values> inputValues =
                WritableObjectChunk.makeWritableChunk(3)) {
            inputValues.setSize(0);
            inputValues.add(genericRecord1);
            inputValues.add(genericRecord2);
            inputValues.add(genericRecord3);

            final WritableChunk[] output = new WritableChunk[7];
            try (final SafeCloseableArray ignored = new SafeCloseableArray(output)) {
                output[0] = WritableObjectChunk.makeWritableChunk(2);
                output[1] = WritableIntChunk.makeWritableChunk(2);
                output[2] = WritableByteChunk.makeWritableChunk(2);
                output[3] = WritableLongChunk.makeWritableChunk(2);
                output[4] = WritableLongChunk.makeWritableChunk(2);
                output[5] = WritableIntChunk.makeWritableChunk(2);
                output[6] = WritableLongChunk.makeWritableChunk(2);

                for (WritableChunk wc : output) {
                    wc.setSize(0);
                }

                final GenericRecordChunkAdapter adapter = GenericRecordChunkAdapter.make(definition,
                        (idx) -> output[idx].getChunkType(), colMap, Pattern.compile(Pattern.quote(".")), avroSchema,
                        true);
                adapter.handleChunk(inputValues, output);

                for (int ii = 0; ii < 7; ++ii) {
                    TestCase.assertEquals(3, output[0].size());
                }

                TestCase.assertEquals("LN1", output[0].asObjectChunk().get(0));
                TestCase.assertEquals(32, output[1].asIntChunk().get(0));
                TestCase.assertEquals(BooleanUtils.FALSE_BOOLEAN_AS_BYTE, output[2].asByteChunk().get(0));
                TestCase.assertEquals(DateTimeUtils.millisToNanos(DateTimeUtils.epochMillis(dt1)),
                        output[3].asLongChunk().get(0));
                TestCase.assertEquals(DateTimeUtils.microsToNanos(DateTimeUtils.epochMicros(dt1)),
                        output[4].asLongChunk().get(0));
                TestCase.assertEquals(10000, output[5].asIntChunk().get(0));
                TestCase.assertEquals(100000, output[6].asLongChunk().get(0));

                TestCase.assertNull(output[0].asObjectChunk().get(1));
                TestCase.assertEquals(64, output[1].asIntChunk().get(1));
                TestCase.assertEquals(BooleanUtils.TRUE_BOOLEAN_AS_BYTE, output[2].asByteChunk().get(1));
                TestCase.assertEquals(DateTimeUtils.millisToNanos(DateTimeUtils.epochMillis(dt2)),
                        output[3].asLongChunk().get(1));
                TestCase.assertEquals(DateTimeUtils.microsToNanos(DateTimeUtils.epochMicros(dt2)),
                        output[4].asLongChunk().get(1));
                TestCase.assertEquals(20000, output[5].asIntChunk().get(1));
                TestCase.assertEquals(200000, output[6].asLongChunk().get(1));

                TestCase.assertEquals("LN3", output[0].asObjectChunk().get(2));
                TestCase.assertEquals(128, output[1].asIntChunk().get(2));
                TestCase.assertEquals(BooleanUtils.NULL_BOOLEAN_AS_BYTE, output[2].asByteChunk().get(2));
                TestCase.assertEquals(QueryConstants.NULL_LONG, output[3].asLongChunk().get(2));
                TestCase.assertEquals(QueryConstants.NULL_LONG, output[4].asLongChunk().get(2));
                TestCase.assertEquals(30000, output[5].asIntChunk().get(2));
                TestCase.assertEquals(300000, output[6].asLongChunk().get(2));
            }
        }
    }
}
