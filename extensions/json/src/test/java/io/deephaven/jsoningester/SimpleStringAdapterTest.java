/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.jsoningester;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.UpdateSourceQueryTable;
import io.deephaven.engine.table.impl.util.DynamicTableWriter;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.NullLoggerImpl;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.TableHeader;
import io.deephaven.qst.type.Type;
import io.deephaven.tablelogger.TableWriter;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import org.junit.*;

import java.io.IOException;
import java.util.function.Function;

import static io.deephaven.engine.util.TableTools.*;

public class SimpleStringAdapterTest {
    @BeforeClass
    static public void setup() {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
    }

    @After
    public void reset() {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testSimple() throws IOException {
        final Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> factory =
                StringMessageToTableAdapter.buildFactory(
                        new NullLoggerImpl(LogLevel.FATAL),
                        new SimpleStringToTableWriterAdapter.Builder().setValueColumnName("a"));

        final String[] names = new String[] {"a"};
        @SuppressWarnings("rawtypes")
        final Class[] types = new Class[] {String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, Type.fromClasses(types));
        final UpdateSourceQueryTable result = writer.getTable();

        final StringMessageToTableAdapter<StringMessageHolder> adapter = factory.apply(writer);

        final TextMessage msg = new TextMessage();

        final String input = "{\"a\": \"Yo\", \"b\": 42.2, \"c\": 123}";
        msg.setText(input);
        adapter.consumeMessage("id", msg);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(result::run);

        final Table expected = newTable(col("a", input));
        Assert.assertEquals("", diff(result, expected, 10));
    }

    @Test
    public void testNullValueColumn() {
        try {
            new SimpleStringToTableWriterAdapter.Builder().setValueColumnName(null).makeAdapter(
                    new NullLoggerImpl(LogLevel.FATAL),
                    new DynamicTableWriter(TableHeader.of(ColumnHeader.ofString("Value"))));
            Assert.fail("buildFactory should have failed on null value column name!");
        } catch (final IllegalArgumentException iae) {
            Assert.assertEquals("Value column must be specified!", iae.getMessage());
        }

    }

    @Test
    public void testInstrumentationSenderAndId() throws IOException {
        final String testCol = "a";
        final String sentCol = "sent";
        final String idCol = "id";
        // Deliberately skipping 'receive' and 'processed' cols for now, because then we need to do a lot more mocking;
        // both are set internally.
        final Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> factory =
                StringMessageToTableAdapter.buildFactory(
                        new NullLoggerImpl(LogLevel.FATAL),
                        new SimpleStringToTableWriterAdapter.Builder()
                                .setValueColumnName(testCol)
                                .sendTimestampColumnName(sentCol)
                                .messageIdColumnName(idCol));

        final String[] names = new String[] {testCol, sentCol, idCol};
        @SuppressWarnings("rawtypes")
        final Class[] types = new Class[] {String.class, DateTime.class, String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, Type.fromClasses(types));
        final UpdateSourceQueryTable result = writer.getTable();

        final StringMessageToTableAdapter<StringMessageHolder> adapter = factory.apply(writer);

        final TextMessage msg = new TextMessage();

        final String input = "{\"a\": \"Yo\", \"b\": 42.2, \"c\": 123}";
        msg.setText(input);
        final DateTime sendTime = DateTime.now();
        final long sendTimeMillis = sendTime.getMillis();
        final DateTime sendTimeTruncated = DateTimeUtils.millisToTime(sendTimeMillis);
        msg.setSenderTimestamp(sendTimeMillis);
        adapter.consumeMessage("id", msg);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(result::run);

        final Table expected = newTable(col(testCol, input), col(sentCol, sendTimeTruncated), col(idCol, "id"));
        Assert.assertEquals("", diff(result, expected, 10));
    }

    @Test
    public void testInstrumentationSendTimeIsNull() throws IOException {
        final String testCol = "a";
        final String sentCol = "sent";
        // Deliberately skipping 'receive' and 'processed' cols for now, because then we need to do a lot more mocking;
        // both are set internally.
        final Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> factory =
                StringMessageToTableAdapter.buildFactory(
                        new NullLoggerImpl(LogLevel.FATAL),
                        new SimpleStringToTableWriterAdapter.Builder()
                                .setValueColumnName(testCol)
                                .sendTimestampColumnName(sentCol));

        final String[] names = new String[] {testCol, sentCol};
        @SuppressWarnings("rawtypes")
        final Class[] types = new Class[] {String.class, DateTime.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, Type.fromClasses(types));
        final UpdateSourceQueryTable result = writer.getTable();

        final StringMessageToTableAdapter<StringMessageHolder> adapter = factory.apply(writer);

        final TextMessage msg = new TextMessage();

        final String input = "{\"a\": \"Yo\", \"b\": 42.2, \"c\": 123}";
        msg.setText(input);
        adapter.consumeMessage("id", msg);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(result::run);

        final Table expected = newTable(col(testCol, input), col(sentCol, (DateTime) null));
        Assert.assertEquals("", diff(result, expected, 10));
    }

    @Test
    public void testInstrumentationReceiveAndTimestamp() throws IOException {
        final String testCol = "a";
        final String rcvCol = "received";
        final String procCol = "processed";
        // Deliberately skipping 'receive' and 'processed' cols for now, because then we need to do a lot more mocking;
        // both are set internally.
        final Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> factory =
                StringMessageToTableAdapter.buildFactory(
                        new NullLoggerImpl(LogLevel.FATAL),
                        new SimpleStringToTableWriterAdapter.Builder()
                                .setValueColumnName(testCol)
                                .receiveTimestampColumnName(rcvCol)
                                .timestampColumnName(procCol));

        final String[] names = new String[] {testCol, rcvCol, procCol};
        @SuppressWarnings("rawtypes")
        final Class[] types = new Class[] {String.class, DateTime.class, DateTime.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, Type.fromClasses(types));
        final UpdateSourceQueryTable result = writer.getTable();

        final StringMessageToTableAdapter<StringMessageHolder> adapter = factory.apply(writer);

        final TextMessage msg = new TextMessage();

        final String input = "{\"a\": \"Yo\", \"b\": 42.2, \"c\": 123}";
        msg.setText(input);
        adapter.consumeMessage("id", msg);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(result::run);

        final Table expected =
                newTable(col(testCol, input), col(rcvCol, (DateTime) null), col(procCol, (DateTime) null));
        final String results = diff(result, expected, 10);
        // The timestamps are variable, so just check that it was different, not the actual value.
        Assert.assertTrue(results
                .contains("Column processed different from the expected set, first difference at row 0 encountered "));
        Assert.assertTrue(results.contains("expected null"));
    }

    @Test
    public void testXMLContentMessage() throws IOException {
        final Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> factory =
                StringMessageToTableAdapter.buildFactory(
                        new NullLoggerImpl(LogLevel.FATAL),
                        new SimpleStringToTableWriterAdapter.Builder().setValueColumnName("a"));

        final String[] names = new String[] {"a"};
        @SuppressWarnings("rawtypes")
        final Class[] types = new Class[] {String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, Type.fromClasses(types));
        final UpdateSourceQueryTable result = writer.getTable();

        final StringMessageToTableAdapter<StringMessageHolder> adapter = factory.apply(writer);

        final TextMessage msg = new TextMessage();

        final String input = "<blah>{\"a\": \"Yo\", \"b\": 42.2, \"c\": 123}</blah>";
        msg.setText(input);
        adapter.consumeMessage("id", msg);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(result::run);

        final Table expected = newTable(col("a", input));
        Assert.assertEquals("", diff(result, expected, 10));
    }

    @Test
    public void testBytesContentMessage() throws IOException {
        final Function<TableWriter<?>, StringMessageToTableAdapter<StringMessageHolder>> factory =
                StringMessageToTableAdapter.buildFactory(
                        new NullLoggerImpl(LogLevel.FATAL),
                        new SimpleStringToTableWriterAdapter.Builder().setValueColumnName("a"));

        final String[] names = new String[] {"a"};
        @SuppressWarnings("rawtypes")
        final Class[] types = new Class[] {String.class};

        final DynamicTableWriter writer = new DynamicTableWriter(names, Type.fromClasses(types));
        final UpdateSourceQueryTable result = writer.getTable();

        final StringMessageToTableAdapter<StringMessageHolder> adapter = factory.apply(writer);

        final TextMessage msg = new TextMessage();

        final String input = "<blah>{\"a\": \"Yo\", \"b\": 42.2, \"c\": 123}</blah>";
        msg.setText(input);
        adapter.consumeMessage("id", msg);

        UpdateGraphProcessor.DEFAULT.runWithinUnitTestCycle(result::run);

        final Table expected = newTable(col("a", input));
        Assert.assertEquals("", diff(result, expected, 10));
    }

    @Test
    public void testIngesterException() {
        final JSONIngesterException je1 = new JSONIngesterException("inner");
        final JSONIngesterException je2 = new JSONIngesterException("outer", je1);
        Assert.assertEquals(je1.getMessage(), je2.getCause().getMessage());
    }
}
