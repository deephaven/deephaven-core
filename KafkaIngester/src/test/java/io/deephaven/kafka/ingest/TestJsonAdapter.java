package io.deephaven.kafka.ingest;

import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.LiveQueryTable;
import io.deephaven.db.v2.utils.DynamicTableWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.function.Function;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.tables.utils.TableTools.col;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;

public class TestJsonAdapter {

    private String json ="{ \"Col1\":5,\"Col2\":3.141592,\"Col3\":\"Hello World\"," +
            "\"Col4\":\"2016-12-31T18:45:01.123 NY\",\"LongValue\":8000000000," +
            "\"FirstName\":\"John\",\"LastName\":\"Doe\",\"empty\":null,\"ISODate\":\"1986-06-02T14:05:45-04:00\"," +
            "\"BigIntValue\":12345678901234567890,\"BigDecimalValue\":1234567890.1234567890," +
            "\"NanoDate\":1577898000000000000, \"BoolValue\":false }";

    @BeforeClass
    static public void setup() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @Before
    public void reset() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testSimple() throws IOException {
        final Function<TableWriter, ConsumerRecordToTableWriterAdapter> factory = new JsonConsumerRecordToTableWriterAdapter.Builder()
                .addColumnToValueField ("c", "Col1")
                .autoValueMapping(true)
                .buildFactory();

        final String [] names = new String[]{"FirstName", "LastName", "c", "Col3", "LongValue", "NanoDate", "BigDecimalValue"};
        final Class [] types = new Class[]{String.class, String.class, int.class, String.class, long.class, DBDateTime.class, BigDecimal.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final ConsumerRecordToTableWriterAdapter jsonAdapter = factory.apply(writer);
        final ConsumerRecord<Object, String> record = new ConsumerRecord<>("topic", 0, 0, null, json);
        jsonAdapter.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(col("FirstName", "John"), col("LastName", "Doe"),
                intCol("c", 5), col("Col3", "Hello World"), col("LongValue", 8000000000L),
                col("NanoDate", new DBDateTime(1577898000000000000L)),
                col("BigDecimalValue", new BigDecimal("1234567890.1234567890")));
        assertTableEquals(expected, result);
    }
}
