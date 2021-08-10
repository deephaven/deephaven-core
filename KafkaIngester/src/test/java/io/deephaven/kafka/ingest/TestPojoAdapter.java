package io.deephaven.kafka.ingest;

import io.deephaven.tablelogger.TableWriter;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.LiveQueryTable;
import io.deephaven.db.v2.utils.DynamicTableWriter;
import io.deephaven.kafka.ingest.pojotest.PojoTest1;
import io.deephaven.kafka.ingest.pojotest.PojoTest2;
import junit.framework.TestCase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.function.Function;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.assertTableEquals;

public class TestPojoAdapter {
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
        final Function<TableWriter, PojoConsumerRecordToStreamPublisherAdapter> factory = new PojoConsumerRecordToStreamPublisherAdapter.Builder()
                .valueClass(PojoTest1.class)
                .addColumnToValueMethod("b", "getB")
                .buildFactory();

        final String [] names = new String[]{"a", "b", "c", "af", "bf", "cf"};
        final Class [] types = new Class[]{String.class, String.class, int.class, String.class, String.class, int.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final PojoConsumerRecordToStreamPublisherAdapter pojoAdapter = factory.apply(writer);
        final ConsumerRecord<Object, PojoTest1> record = new ConsumerRecord<>("topic", 0, 0, null, new PojoTest1());
        pojoAdapter.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(col("a", "A"), col("b", "B"), intCol("c", 1), col("af", "AF"), col("bf", "BF"), col("cf", 7));
        assertTableEquals(expected, result);
    }

    @Test
    public void testSimple2() throws IOException {
        final Function<TableWriter, PojoConsumerRecordToStreamPublisherAdapter> factory = new PojoConsumerRecordToStreamPublisherAdapter.Builder()
                .keyClass(PojoTest1.class)
                .addColumnToKeyMethod("b", "getB")
                .addColumnToKeyField("c", "cf")
                .caseInsensitiveSearch(true)
                .buildFactory();

        final String [] names = new String[]{"a", "b", "c", "Af", "bf", "cf"};
        final Class [] types = new Class[]{String.class, String.class, int.class, String.class, String.class, int.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final PojoConsumerRecordToStreamPublisherAdapter pojoAdapter = factory.apply(writer);
        final ConsumerRecord<PojoTest1, Object> record = new ConsumerRecord<>("topic", 0, 0, new PojoTest1(), null);
        pojoAdapter.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(col("a", "A"), col("b", "B"), intCol("c", 7), col("Af", "AF"), col("bf", "BF"), col("cf", 7));
        assertTableEquals(expected, result);
    }

    @Test
    public void testFuzzyMatch() throws IOException {
        final PojoConsumerRecordToStreamPublisherAdapter.Builder builder =
                new PojoConsumerRecordToStreamPublisherAdapter.Builder()
                        .valueClass(PojoTest1.class)
                        .caseInsensitiveSearch(true);
        final Function<TableWriter, PojoConsumerRecordToStreamPublisherAdapter> factory = builder.buildFactory();

        final String [] names = new String[]{"A", "B", "C", "AF", "Bf", "CF"};
        final Class [] types = new Class[]{String.class, String.class, int.class, String.class, String.class, int.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();

        final PojoConsumerRecordToStreamPublisherAdapter pojoAdapter = factory.apply(writer);
        final ConsumerRecord<Object, PojoTest1> record = new ConsumerRecord<>("topic", 0, 0, null, new PojoTest1());
        pojoAdapter.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(col("A", "A"), col("B", "B"), intCol("C", 1), col("AF", "AF"), col("Bf", "BF"), col("CF", 7));
        assertTableEquals(expected, result);
    }

    @Test
    public void testUnmapped() {
        final PojoConsumerRecordToStreamPublisherAdapter.Builder builder = new PojoConsumerRecordToStreamPublisherAdapter.Builder();
        builder.valueClass(PojoTest1.class).addColumnToValueMethod("b", "getB");
        final Function<TableWriter, PojoConsumerRecordToStreamPublisherAdapter> factory = builder.buildFactory();

        final String [] names = new String[]{"a", "b", "c", "af", "bf", "cf", "df"};
        final Class [] types = new Class[]{String.class, String.class, int.class, String.class, String.class, int.class, long.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        try {
            factory.apply(writer);
            TestCase.fail("Expected exception");
        } catch (RuntimeException iae) {
            TestCase.assertEquals("No fields found for columns: [df=long]", iae.getMessage());
        }
    }

    @Test
    public void testSpecialColumns() throws IOException {
        final PojoConsumerRecordToStreamPublisherAdapter.Builder builder = new PojoConsumerRecordToStreamPublisherAdapter.Builder();
        builder.valueClass(PojoTest1.class).keyClass(PojoTest2.class).kafkaPartitionColumnName("KP").offsetColumnName("Offset");
        final Function<TableWriter, PojoConsumerRecordToStreamPublisherAdapter> factory = builder.buildFactory();

        final String [] names = new String[]{"a", "b", "c", "df", "KP", "Offset"};
        final Class [] types = new Class[]{String.class, String.class, int.class, int.class, int.class, long.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, types);
        final LiveQueryTable result = writer.getTable();


        final PojoConsumerRecordToStreamPublisherAdapter pojoAdapter = factory.apply(writer);
        final ConsumerRecord<PojoTest2, PojoTest1> record = new ConsumerRecord<>("topic", 1, 27, new PojoTest2(), new PojoTest1());
        pojoAdapter.consumeRecord(record);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(result::refresh);

        final Table expected = newTable(col("a", "A"), col("b", "B2"), intCol("c", 1), intCol("df", 9), intCol("KP", 1), longCol("Offset", 27));
        assertTableEquals(expected, result);
    }
}
