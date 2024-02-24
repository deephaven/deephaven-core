package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BlinkTableTools;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.function.Random;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.StreamLoggerImpl;
import io.deephaven.jsoningester.msg.MessageMetadata;
import io.deephaven.stream.StreamToBlinkTableAdapter;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.process.FatalErrorReporter;
import io.deephaven.util.process.ProcessEnvironment;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_LONG;

/**
 * Tests the {@link JSONToStreamPublisherAdapter} and {@link JSONToStreamPublisherAdapterBuilder}.
 * <p>
 * Note that some tests are run with a consumer thread pool (as would be used in production), and others use zero
 * threads ({@link JSONToStreamPublisherAdapterBuilder#nConsumerThreads(int)
 * JSONToStreamPublisherAdapterBuilder#nConsumerThreads(0)}) and consume/process messages synchronously (which is
 * helpful for catching exceptions and producing helpful stack traes in the unit tests).
 */
public class JsonStreamAdapterTest extends RefreshingTableTestCase {
    private static final double NANOS_PER_SECOND = 1000000000.0;
    private static final long MAX_WAIT_MILLIS = TimeUnit.SECONDS.toMillis(300);
    private static final long MESSAGE_TIMESTAMP_MILLIS = 1601578523551L;

    private final Logger log = LoggerFactory.getLogger(JsonStreamAdapterTest.class);

    // For convenience, we want to be able to kill the consumer daemons and clear queues.
    // TODO: this is probably not being used properly, since converting from JsonAdapterTest. Use try-with-resources?
    private StringMessageToBlinkTableAdapter<StringMessageHolder> adapter = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        if (ProcessEnvironment.tryGet() == null) {
            ProcessEnvironment.basicServerInitialization(Configuration.getInstance(),
                    JsonStreamAdapterTest.class.getName(), new StreamLoggerImpl());
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (adapter != null) {
            adapter.shutdown();
        }
        adapter = null;

        if (ProcessEnvironment.tryGet() != null) {
            ProcessEnvironment.clear();
        }
    }

    public void testAutomap() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder());

        final String strCol = "str";
        final String dblCol = "dbl";
        final String intCol = "iger";
        final String shortCol = "shrt";
        final String longCol = "lng";
        final String byteCol = "byt";
        final String floatColName = "flt";
        final String boolColName = "bln";
        final String charColName = "cha";

        final List<String> names = Arrays.asList(strCol, dblCol, intCol, shortCol, longCol, byteCol, floatColName,
                boolColName, charColName);
        final List<Class<?>> types =
                Arrays.asList(String.class, double.class, int.class, short.class, long.class, byte.class,
                        float.class, boolean.class, char.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"" + strCol + "\": \"test\", \""
                + dblCol + "\": 42.2, \""
                + intCol + "\": 123, \""
                + shortCol + "\": 6, \""
                + longCol + "\": 123456789, \""
                + byteCol + "\": 3, \""
                + floatColName + "\": 98765.4321, \""
                + boolColName + "\": true, \""
                + charColName + "\": \"c\""
                + "}", "id", tableAndPublisher);

        final Table expected = newTable(col(strCol, "test"),
                doubleCol(dblCol, 42.2),
                intCol(intCol, 123),
                shortCol(shortCol, (short) 6),
                longCol(longCol, 123456789),
                byteCol(byteCol, (byte) 3),
                floatCol(floatColName, (float) 98765.4321),
                col(boolColName, true),
                col(charColName, 'c')

        );

        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testAutomapNulls() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .allowNullValues(true)
                                .nConsumerThreads(0));

        final String strCol = "str";
        final String dblCol = "dbl";
        final String intCol = "iger";
        final String shortCol = "shrt";
        final String longCol = "lng";
        final String byteCol = "byt";
        final String floatColName = "flt";
        final String boolColName = "bln";
        final String charColName = "cha";

        final List<String> names = Arrays.asList(strCol, dblCol, intCol, shortCol, longCol, byteCol, floatColName,
                boolColName, charColName);
        final List<Class<?>> types =
                Arrays.asList(String.class, double.class, int.class, short.class, long.class, byte.class,
                        float.class, boolean.class, char.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"" + strCol + "\": null, \""
                + dblCol + "\": null, \""
                + intCol + "\": null, \""
                + shortCol + "\": null, \""
                + longCol + "\": null, \""
                + byteCol + "\": null, \""
                + floatColName + "\": null, \""
                + boolColName + "\": null, \""
                + charColName + "\": null"
                + "}", "id", tableAndPublisher);

        final Table expected = newTable(col(strCol, new String[] {null}),
                doubleCol(dblCol, QueryConstants.NULL_DOUBLE),
                intCol(intCol, QueryConstants.NULL_INT),
                shortCol(shortCol, QueryConstants.NULL_SHORT),
                longCol(longCol, NULL_LONG),
                byteCol(byteCol, QueryConstants.NULL_BYTE), // ASCII value of character '3' is the byte value 51
                floatCol(floatColName, QueryConstants.NULL_FLOAT),
                col(boolColName, QueryConstants.NULL_BOOLEAN),
                col(charColName, QueryConstants.NULL_CHAR));

        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testAutomapWithSenderTimestamp() throws IOException, InterruptedException, TimeoutException {
        final String strCol = "str";
        final String dblCol = "dbl";
        final String intCol = "iger";
        final String shortCol = "shrt";
        final String longCol = "lng";
        final String byteCol = "byt";
        final String floatColName = "flt";
        final String boolColName = "bln";
        final String charColName = "cha";
        final String sendCol = "sent";

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log,
                        new JSONToStreamPublisherAdapterBuilder().sendTimestampColumnName(sendCol));

        final List<String> names = Arrays.asList(strCol, dblCol, intCol, shortCol, longCol, byteCol, floatColName,
                boolColName, charColName, sendCol);
        final List<Class<?>> types =
                Arrays.asList(String.class, double.class, int.class, short.class, long.class, byte.class,
                        float.class, boolean.class, char.class, Instant.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        final Instant sendTime = Instant.now();
        final long sendTimeMillis = sendTime.toEpochMilli();

        final StringMessageHolder msg = new StringMessageHolder(sendTimeMillis * 1000L,
                "{\"" + strCol + "\": \"test\", \""
                        + dblCol + "\": 42.2, \""
                        + intCol + "\": 123, \""
                        + shortCol + "\": 6, \""
                        + longCol + "\": 123456789, \""
                        + byteCol + "\": 3, \""
                        + floatColName + "\": 98765.4321, \""
                        + boolColName + "\": true, \""
                        + charColName + "\": \"c\""
                        + "}");

        adapter.consumeMessage("id", msg);

        // Because the message will be consumed almost instantly, then actually processed separately, we have to wait to
        // see the results.
        adapter.waitForProcessing(MAX_WAIT_MILLIS);
        adapter.cleanup();

        updateBlinkTables(tableAndPublisher);

        TableTools.show(result);

        final Table expected = newTable(col(strCol, "test"),
                doubleCol(dblCol, 42.2),
                intCol(intCol, 123),
                shortCol(shortCol, (short) 6),
                longCol(longCol, 123456789),
                byteCol(byteCol, (byte) 3),
                floatCol(floatColName, (float) 98765.4321),
                col(boolColName, true),
                col(charColName, 'c'),
                col(sendCol, DateTimeUtils.epochMillisToInstant(sendTimeMillis)));

        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testAutomapInvalidType() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder());

        final String strCol = "str";
        final String objCol = "obj";

        final List<String> names = Arrays.asList(strCol, objCol);
        final List<Class<?>> types = Arrays.asList(String.class, Object.class);

        try (final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types)) {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should not have been able to auto-create Object-type column!");
        } catch (final UnsupportedOperationException uoe) {
            Assert.assertEquals("Can not convert JSON field to class java.lang.Object for column " + objCol + " (field "
                    + objCol + ")", uoe.getMessage());
        }
    }

    public void testNullBoolean() throws IOException, InterruptedException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log,
                        new JSONToStreamPublisherAdapterBuilder().allowNullValues(true)
                                .nConsumerThreads(0));

        final String strCol = "str";
        final String boolColName = "bln";

        final List<String> names = Arrays.asList(strCol, boolColName);
        final List<Class<?>> types = Arrays.asList(String.class, Boolean.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        final StringMessageHolder msg = new StringMessageHolder("{\"" + strCol + "\": \"test\", \""
                + boolColName + "\": null "
                + "}");

        adapter.consumeMessage("id", msg);

        // Because the message will be consumed almost instantly, then actually processed separately, we have to wait to
        // see the results.
        Thread.sleep(200);
        adapter.cleanup();

        updateBlinkTables(tableAndPublisher);

        final Table expected = newTable(col(strCol, "test"),
                ColumnHolder.getBooleanColumnHolder(boolColName, false, (byte) 2));

        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testExtraneousMappings() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromField("noSuchColumns", "ignored"));

        final String strCol = "str";

        final List<String> names = Arrays.asList(strCol);
        final List<Class<?>> types = Arrays.asList(String.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);

        try {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should not have been able to include mapping to nonexistent column!");
        } catch (final JSONIngesterException jie) {
            Assert.assertEquals("Found mappings that do not correspond to this table: [noSuchColumns]",
                    jie.getMessage());
        }
    }

    public void testMissing() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log,
                        new JSONToStreamPublisherAdapterBuilder().autoValueMapping(false));

        final List<String> names = Arrays.asList("a", "b", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);

        try {
            factory.apply(tableAndPublisher.getPublisher());
            TestCase.fail();
        } catch (final JSONIngesterException e) {
            TestCase.assertEquals("Found columns without mappings [a, b, c]", e.getMessage());
        }
    }

    public void testWithMissingKeys() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowMissingKeys(true)
                        .nConsumerThreads(0));

        final List<String> names = Arrays.asList("a", "b", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"test\", \"b\": 42.2}", "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(col("a", "test"), doubleCol("b", 42.2), intCol("c", QueryConstants.NULL_INT));
        final String diffValue = diff(result, expected, 10);
        Assert.assertEquals("", diffValue);

        updateBlinkTables(tableAndPublisher);
    }

    public void testWithNullInt() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log,
                        new JSONToStreamPublisherAdapterBuilder().allowNullValues(true));

        final List<String> names = Arrays.asList("a", "b", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"test\", \"b\": 42.2, \"c\": null}", "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(col("a", "test"), doubleCol("b", 42.2), intCol("c", QueryConstants.NULL_INT));
        final String diffValue = diff(result, expected, 10);
        Assert.assertEquals("", diffValue);

        updateBlinkTables(tableAndPublisher);
    }

    public void testWithNullString() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log,
                        new JSONToStreamPublisherAdapterBuilder().allowNullValues(true));

        final List<String> names = Arrays.asList("a", "b", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, String.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"test\", \"b\": 42.2, \"c\": null}", "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(col("a", "test"), doubleCol("b", 42.2), stringCol("c", new String[] {null}));
        final String diffValue = diff(result, expected, 10);
        Assert.assertEquals("", diffValue);

        updateBlinkTables(tableAndPublisher);
    }

    public void testTimes() throws IOException, InterruptedException, TimeoutException {
        final Instant reference = DateTimeUtils.parseInstant("2020-06-25T09:37:00.123456789 NY");

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder());

        final List<String> names = Arrays.asList("Nanos", "Micros", "Millis", "StringVal");
        final List<Class<?>> types = Arrays.asList(Instant.class, Instant.class, Instant.class, Instant.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        // @formatter:off
        final StringMessageHolder msg = new StringMessageHolder(
                "{\"StringVal\": \"" + reference +
                        "\", \"Nanos\": " + reference.getEpochSecond() + reference.getNano() +
                        ", \"Micros\":" + reference.getEpochSecond() + reference.getNano() / 1000 +
                        ", \"Millis\": " + reference.getEpochSecond() + reference.getNano() / 1000000 +
                        "}");
        // @formatter:on

        adapter.consumeMessage("id", msg);

        adapter.waitForProcessing(MAX_WAIT_MILLIS);
        adapter.cleanup();

        updateBlinkTables(tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(
                col("Nanos", reference),
                col("Micros", DateTimeUtils.parseInstant("2020-06-25T09:37:00.123456 NY")),
                col("Millis", DateTimeUtils.parseInstant("2020-06-25T09:37:00.123 NY")),
                col("StringVal", reference));
        // @formatter:on
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testSimpleMapping() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B").addColumnFromField("A", "a"));

        final List<String> names = Arrays.asList("A", "B", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"test\", \"b\": 42.2, \"c\": 123}", "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(col("A", "test"), doubleCol("B", QueryConstants.NULL_DOUBLE), intCol("c", 123));
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testPrimitiveParallel() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromField("A", "a")
                        .addColumnFromField("B", "b")
                        .addFieldParallel("C", "c")
                        .addFieldParallel("D", "d")
                        .autoValueMapping(false)
                        .allowNullValues(true));

        final List<String> names = Arrays.asList("A", "B", "C", "D");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"test\", \"b\": 42.2, \"c\": [2020, 2021], \"d\": [\"Foo\", \"Bar\"] }", "id",
                tableAndPublisher);

        Assert.assertEquals(2, result.intSize());
        final Table expected = newTable(
                // expanded out to each row
                col("A", "test", "test"), doubleCol("B", 42.2, 42.2),
                // the parallel fields
                longCol("C", 2020, 2021), stringCol("D", "Foo", "Bar"));
        Assert.assertEquals("", diff(result, expected, 10));


        injectJson("{\"a\": \"Ahoy\", \"b\": 47, \"c\": [2022, 2023, 2024], \"d\": null }", "id2", tableAndPublisher);

        final Table expected2 = newTable(
                // expanded out to each row
                col("A", "test", "test", "Ahoy", "Ahoy", "Ahoy"), doubleCol("B", 42.2, 42.2, 47, 47, 47),
                // the parallel fields
                longCol("C", 2020, 2021, 2022, 2023, 2024), stringCol("D", "Foo", "Bar", null, null, null));
        Assert.assertEquals("", diff(result, expected2, 10));

        injectJson("{\"a\": \"Greetings\", \"b\": 112358, \"c\": null, \"d\": null }", "id2", tableAndPublisher);

        final Table expected3 = newTable(
                // expanded out to each row
                col("A", "test", "test", "Ahoy", "Ahoy", "Ahoy", "Greetings"),
                doubleCol("B", 42.2, 42.2, 47, 47, 47, 112358),
                // the parallel fields
                longCol("C", 2020, 2021, 2022, 2023, 2024, NULL_LONG),
                stringCol("D", "Foo", "Bar", null, null, null, null));
        Assert.assertEquals("", diff(result, expected3, 10));

        injectJson("{\"a\": \"Salutations\", \"b\": 132235, \"c\": null, \"d\": [\"Baz\"] }", "id2", tableAndPublisher);

        final Table expected4 = newTable(
                // expanded out to each row
                col("A", "test", "test", "Ahoy", "Ahoy", "Ahoy", "Greetings", "Salutations"),
                doubleCol("B", 42.2, 42.2, 47, 47, 47, 112358, 132235),
                // the parallel fields
                longCol("C", 2020, 2021, 2022, 2023, 2024, NULL_LONG, NULL_LONG),
                stringCol("D", "Foo", "Bar", null, null, null, null, "Baz"));
        Assert.assertEquals("", diff(result, expected4, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testParallelTypes() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromField("Expanded", "toExpand")
                        .addFieldParallel("ByteCol", "byte")
                        .addFieldParallel("CharCol", "char")
                        .addFieldParallel("ShortCol", "short")
                        .addFieldParallel("IntCol", "int")
                        .addFieldParallel("LongCol", "long")
                        .addFieldParallel("FloatCol", "float")
                        .addFieldParallel("DoubleCol", "double")
                        .addFieldParallel("StringCol", "str")
                        .addFieldParallel("BoolCol", "bool")
                        .addFieldParallel("DTCol", "dt")
                        .autoValueMapping(false)
                        .allowNullValues(true));

        final List<String> names = Arrays.asList("Expanded", "ByteCol", "CharCol", "ShortCol", "IntCol", "LongCol",
                "FloatCol", "DoubleCol", "StringCol", "BoolCol", "DTCol");
        final List<Class<?>> types =
                Arrays.asList(String.class, byte.class, char.class, short.class, int.class, long.class,
                        float.class, double.class, String.class, Boolean.class, Instant.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"toExpand\": \"expanded\", \"byte\": [1, null, 2], \"char\": [\"A\", null, \"B\"], \"short\": [3, null, 4], \"int\": [5, null, 6], \"long\": [7, null, 8], \"float\": [9.9, null, 10.1], \"double\": [11.11, null, 12.12], \"str\":  [null, \"Billy\", \"Willy\"], \"bool\": [true, false, null], \"dt\": [ 1600348073000, 1600348077000, null ] }",
                "id", tableAndPublisher);

        final Table expected = newTable(
                // expanded out to each row
                col("Expanded", "expanded", "expanded", "expanded"),
                // the parallel fields
                byteCol("ByteCol", (byte) 1, QueryConstants.NULL_BYTE, (byte) 2),
                charCol("CharCol", 'A', QueryConstants.NULL_CHAR, 'B'),
                shortCol("ShortCol", (short) 3, QueryConstants.NULL_SHORT, (short) 4),
                intCol("IntCol", 5, QueryConstants.NULL_INT, 6),
                longCol("LongCol", 7, NULL_LONG, 8),
                floatCol("FloatCol", 9.9f, QueryConstants.NULL_FLOAT, 10.10f),
                doubleCol("DoubleCol", 11.11, QueryConstants.NULL_DOUBLE, 12.12),
                stringCol("StringCol", null, "Billy", "Willy"),
                col("BoolCol", true, false, null),
                col("DTCol",
                        DateTimeUtils.epochSecondsToInstant(1600348073L),
                        DateTimeUtils.epochSecondsToInstant(1600348077L),
                        null));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testNestedRecordParallel() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factoryNestedHi = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("H", "h")
                .addColumnFromField("I", "i")
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("A", "a")
                        .addNestedFieldParallel("c", factoryNestedDe)
                        .addFieldParallel("F", "f")
                        .addNestedField("g", factoryNestedHi)
                        .autoValueMapping(false)
                        .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "B", "D", "E", "F", "H", "I");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class, int.class,
                String.class, String.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"c\": [ {\"d\": 123, \"e\": \"Foo\"}, {\"d\": 456, \"e\": \"Bar\"} ], \"f\": [2020, 2021], \"g\": { \"h\": \"chuckie\", \"i\": \"carlos\" } }",
                "id", tableAndPublisher);

        Assert.assertEquals(2, result.intSize());
        final Table expected = newTable(
                // expanded out to each row
                col("A", "test", "test"), doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                // the nested parallel fields
                longCol("D", 123, 456), stringCol("E", "Foo", "Bar"), intCol("F", 2020, 2021),
                // the nested singleton field expanded to each row
                stringCol("H", "chuckie", "chuckie"), stringCol("I", "carlos", "carlos"));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        injectJson(
                "{\"a\": \"Ahoy\", \"b\": 44.2, \"c\": [ {\"d\": 124, \"e\": \"Baz\"}, null, {\"d\": 457, \"e\": \"Quux\"} ], \"f\": [2022, 2023, 2024], \"g\": { \"h\": \"chas\", \"i\": \"karl\" } }",
                "id", tableAndPublisher);

        final Table expected2 = newTable(
                // expanded out to each row
                col("A", "test", "test", "Ahoy", "Ahoy", "Ahoy"),
                doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                // the nested parallel fields
                longCol("D", 123, 456, 124, NULL_LONG, 457), stringCol("E", "Foo", "Bar", "Baz", null, "Quux"),
                intCol("F", 2020, 2021, 2022, 2023, 2024),
                // the nested singleton field expanded to each row
                stringCol("H", "chuckie", "chuckie", "chas", "chas", "chas"),
                stringCol("I", "carlos", "carlos", "karl", "karl", "karl"));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected2, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testNestedRecord() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(false);
        final JSONToStreamPublisherAdapterBuilder factoryNestedGh = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("G", "g")
                .addColumnFromField("H", "h")
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("I", "b")
                        .addColumnFromField("A", "a")
                        .addNestedField("c", factoryNestedDe)
                        .addNestedField("f", factoryNestedGh)
                        .autoValueMapping(false));

        final List<String> names = Arrays.asList("A", "B", "D", "E", "G", "H", "I");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class, short.class,
                float.class, double.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"c\": {\"d\": 123, \"e\": \"Foo\"}, \"f\": {\"g\": 456, \"h\": 3.14 } }",
                "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(
                col("A", "test"), doubleCol("B", QueryConstants.NULL_DOUBLE),
                longCol("D", 123), stringCol("E", "Foo"), shortCol("G", (short) 456), floatCol("H", 3.14f),
                doubleCol("I", 42.2));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testJsonPointers() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .nConsumerThreads(0)
                        .addColumnFromField("A0", "a")
                        .addColumnFromField("B0", "b")
                        .addColumnFromPointer("A", "/a")
                        .addColumnFromPointer("B", "/b")
                        .addColumnFromPointer("D", "/c/d")
                        .addColumnFromPointer("E", "/c/e")
                        .addColumnFromPointer("E", "/c/e")
                        .addColumnFromPointer("G", "/f/g")
                        .addColumnFromPointer("H", "/f/h")
                        .autoValueMapping(false));

        final List<String> names = Arrays.asList("A0", "B0", "A", "B", "D", "E", "G", "H");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, String.class, double.class, long.class,
                String.class, short.class, float.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"c\": {\"d\": 123, \"e\": \"Foo\"}, \"f\": {\"g\": 456, \"h\": 3.14 } }",
                "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(
                col("A0", "test"),
                doubleCol("B0", 42.2),
                col("A", "test"),
                doubleCol("B", 42.2),
                longCol("D", 123),
                stringCol("E", "Foo"),
                shortCol("G", (short) 456),
                floatCol("H", 3.14f));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testJsonPointers2() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D0", "d")
                .addColumnFromField("E0", "e")
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .nConsumerThreads(0)
                        .addColumnFromField("A0", "a")
                        .addColumnFromPointer("A1", "/a")
                        .addNestedField("c", factoryNestedDe)
                        .addColumnFromPointer("D1", "/c/d")
                        .addColumnFromPointer("E1", "/c/e")
                        .autoValueMapping(false));

        final List<String> names = Arrays.asList("A0", "A1", "D0", "E0", "D1", "E1");
        final List<Class<?>> types =
                Arrays.asList(String.class, String.class, long.class, String.class, long.class, String.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"test\", \"c\": {\"d\": 123, \"e\": \"Foo\"} }", "id", tableAndPublisher);

        Assert.assertEquals(1, result.intSize());
        final Table expected = newTable(
                col("A0", "test"),
                col("A1", "test"),
                longCol("D0", 123),
                stringCol("E0", "Foo"),
                longCol("D1", 123),
                stringCol("E1", "Foo"));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testSubtables() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factorySubtableDe = new JSONToStreamPublisherAdapterBuilder()
                .allowMissingKeys(true)
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(false);

        final StreamPublisherAndTable resultSubtable = createStreamPublisherAndTable(
                Arrays.asList("D", "E", "SubtableRecordId"),
                Arrays.asList(long.class, String.class, long.class));

        final BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactoryWithSubtables(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .allowUnmapped("B")
                                .addColumnFromField("A", "a")
                                .addFieldToSubTableMapping("c", factorySubtableDe)
                                .autoValueMapping(false)
                                .nConsumerThreads(0));

        final StreamPublisherAndTable resultMain = createStreamPublisherAndTable(
                Arrays.asList("A", "B", "c_id"),
                Arrays.asList(String.class, double.class, long.class));

        adapter = factory.apply(resultMain.getPublisher(), Map.of("c", resultSubtable.getPublisher()));

        final Table mainTableBlink = resultMain.getBlinkTable();
        final Table subtableBlink = resultSubtable.getBlinkTable();

        final Table mainTable = resultMain.getAppendOnlyTable();
        final Table subtable = resultSubtable.getAppendOnlyTable();

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"c\": [ {\"d\": 123, \"e\": \"Foo\"}, {\"d\": 456, \"e\": \"Bar\"} ]}",
                "id", resultMain, resultSubtable);

        {
            // Check the main table:
            TableTools.show(mainTable);
            Assert.assertEquals(1, mainTable.intSize());
            final Table expectedMain = newTable(
                    col("A", "test"),
                    doubleCol("B", QueryConstants.NULL_DOUBLE),
                    // the subtable row IDs
                    longCol("c_id", 0));
            Assert.assertEquals("", diff(mainTable, expectedMain, 10));

            // Check the subtable:
            TableTools.show(subtable);
            Assert.assertEquals(2, subtable.intSize());
            final Table expectedSubtable = newTable(
                    longCol("D", 123, 456),
                    stringCol("E", "Foo", "Bar"),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 0, 0));
            Assert.assertEquals("", diff(subtable, expectedSubtable, 10));
        }

        injectJson(
                "{\"a\": \"test2\", \"b\": 42.2, \"c\": [ {\"d\": 124, \"e\": \"Baz\"}, null, {\"d\": 457, \"e\": \"Quux\"} ]}",
                "id", resultMain, resultSubtable);

        {
            // Check the main table:
            System.out.println("Main table:");
            TableTools.show(mainTable);
            Assert.assertEquals(2, mainTable.intSize());
            final Table expectedMain = newTable(
                    col("A", "test", "test2"),
                    doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                    // the subtable row IDs
                    longCol("c_id", 0, 1));
            Assert.assertEquals("", diff(mainTable, expectedMain, 10));

            // Check the subtable:
            TableTools.show(subtable);
            Assert.assertEquals(5, subtable.intSize());
            final Table expectedSubtable = newTable(
                    longCol("D", 123, 456, 124, QueryConstants.NULL_LONG, 457),
                    stringCol("E", "Foo", "Bar", "Baz", null, "Quux"),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 0, 0, 1, 1, 1));
            Assert.assertEquals("", diff(subtable, expectedSubtable, 10));
        }

        updateBlinkTables(resultMain.getAdapter(), resultSubtable.getAdapter());

        resultMain.close();
        resultSubtable.close();

        updateBlinkTables(resultMain, resultSubtable);
    }

    public void testSubtablesAndNestedFields() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factorySubtableDe = new JSONToStreamPublisherAdapterBuilder()
                .allowMissingKeys(true)
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .addNestedField("X1",
                        new JSONToStreamPublisherAdapterBuilder().autoValueMapping(false).addColumnFromField("X1_y",
                                "y"))
                .autoValueMapping(false);

        final StreamPublisherAndTable subtableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList("D", "E", "X1_y", "SubtableRecordId"),
                Arrays.asList(long.class, String.class, int.class, long.class));
        final Table resultSubtable = subtableAndPublisher.getAppendOnlyTable();

        final BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactoryWithSubtables(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .allowUnmapped("B")
                                .addColumnFromField("A", "a")
                                .addFieldToSubTableMapping("c", factorySubtableDe)
                                .addNestedField("X2",
                                        new JSONToStreamPublisherAdapterBuilder().autoValueMapping(false)
                                                .addColumnFromField("X2_y",
                                                        "y"))
                                .autoValueMapping(false)
                                .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "B", "X2_y", "c_id");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class, long.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table resultMain = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher(), Map.of("c", subtableAndPublisher.getPublisher()));

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"X2\": { \"y\": 100 }, \"c\": [ {\"d\": 123, \"e\": \"Foo\", \"X1\": { \"y\": 7 }}, {\"d\": 456, \"e\": \"Bar\"} ], \"X1\": { \"y\": 7 }}",
                "id", tableAndPublisher, subtableAndPublisher);

        {
            // Check the main table:
            TableTools.show(resultMain);
            Assert.assertEquals(1, resultMain.intSize());
            final Table expectedMain = newTable(
                    col("A", "test"),
                    doubleCol("B", QueryConstants.NULL_DOUBLE),
                    intCol("X2_y", 100),
                    // the subtable row IDs
                    longCol("c_id", 0));
            Assert.assertEquals("", diff(resultMain, expectedMain, 10));

            // Check the subtable:
            TableTools.show(resultSubtable);
            Assert.assertEquals(2, resultSubtable.intSize());
            final Table expectedSubtable = newTable(
                    longCol("D", 123, 456),
                    stringCol("E", "Foo", "Bar"),
                    intCol("X1_y", 7, QueryConstants.NULL_INT),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 0, 0));
            Assert.assertEquals("", diff(resultSubtable, expectedSubtable, 10));
        }

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"X2\": { \"y\": 101 }, \"c\": [ {\"d\": 124, \"e\": \"Baz\", \"X1\": { \"y\": -7 }}, null, {\"d\": 457, \"e\": \"Quux\"} ]}",
                "id", tableAndPublisher, subtableAndPublisher);

        {
            // Check the main table:
            TableTools.show(resultMain);
            Assert.assertEquals(2, resultMain.intSize());
            final Table expectedMain = newTable(
                    col("A", "test", "test"),
                    doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                    intCol("X2_y", 100, 101),
                    // the subtable row IDs
                    longCol("c_id", 0, 1));
            Assert.assertEquals("", diff(resultMain, expectedMain, 10));

            // Check the subtable:
            TableTools.show(resultSubtable);
            Assert.assertEquals(5, resultSubtable.intSize());
            final Table expectedSubtable = newTable(
                    longCol("D", 123, 456, 124, QueryConstants.NULL_LONG, 457),
                    stringCol("E", "Foo", "Bar", "Baz", null, "Quux"),
                    intCol("X1_y", 7, QueryConstants.NULL_INT, -7, QueryConstants.NULL_INT, QueryConstants.NULL_INT),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 0, 0, 1, 1, 1));
            Assert.assertEquals("", diff(resultSubtable, expectedSubtable, 10));
        }

        updateBlinkTables(tableAndPublisher, subtableAndPublisher);
    }

    public void testSubtablesAndNestedFields2() throws IOException, InterruptedException, TimeoutException {
        // test to catch a bug where a nested field's adapter can't be built if a sibling nested field includes a
        // subtable

        final JSONToStreamPublisherAdapterBuilder factorySubtableDe = new JSONToStreamPublisherAdapterBuilder()
                .allowMissingKeys(true)
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(false);

        final StreamPublisherAndTable subtableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList("D", "E", "SubtableRecordId"),
                Arrays.asList(long.class, String.class, long.class));
        final Table resultSubtable = subtableAndPublisher.getAppendOnlyTable();

        final BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactoryWithSubtables(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .allowUnmapped("B")
                                .addColumnFromField("A", "a")
                                .addNestedField("X1",
                                        new JSONToStreamPublisherAdapterBuilder().autoValueMapping(false)
                                                .addColumnFromField("X1_y",
                                                        "y"))
                                .addNestedField("X2",
                                        new JSONToStreamPublisherAdapterBuilder().autoValueMapping(false)
                                                .addFieldToSubTableMapping("c", factorySubtableDe))
                                .autoValueMapping(false)
                                .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "B", "X1_y", "c_id");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class, long.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table resultMain = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher(), Map.of("c", subtableAndPublisher.getPublisher()));

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"X1\": { \"y\": 7 }, \"X2\": { \"c\": [ {\"d\": 123, \"e\": \"Foo\", \"X1\": { \"y\": 7 }}, {\"d\": 456, \"e\": \"Bar\"} ]}}",
                "id", tableAndPublisher, subtableAndPublisher);

        {
            // Check the main table:
            TableTools.show(resultMain);
            Assert.assertEquals(1, resultMain.intSize());
            final Table expectedMain = newTable(
                    col("A", "test"),
                    doubleCol("B", QueryConstants.NULL_DOUBLE),
                    intCol("X1_y", 7),
                    // the subtable row IDs
                    longCol("c_id", 0));
            Assert.assertEquals("", diff(resultMain, expectedMain, 10));

            // Check the subtable:
            TableTools.show(resultSubtable);
            Assert.assertEquals(2, resultSubtable.intSize());
            final Table expectedSubtable = newTable(
                    longCol("D", 123, 456),
                    stringCol("E", "Foo", "Bar"),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 0, 0));
            Assert.assertEquals("", diff(resultSubtable, expectedSubtable, 10));
        }

        updateBlinkTables(tableAndPublisher, subtableAndPublisher);
    }

    public void testSubtableMissingKeys() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factorySubtableDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(false);

        final StreamPublisherAndTable subtableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList("D", "E", "SubtableRecordId"),
                Arrays.asList(long.class, String.class, long.class));
        final Table resultSubtable = subtableAndPublisher.getAppendOnlyTable();

        final BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactoryWithSubtables(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .allowUnmapped("B")
                                .addColumnFromField("A", "a")
                                .addFieldToSubTableMapping("c", factorySubtableDe)
                                .autoValueMapping(false)
                                .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "B", "c_id");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);

        adapter = factory.apply(tableAndPublisher.getPublisher(), Map.of("c", subtableAndPublisher.getPublisher()));

        try {
            injectJson(
                    "{\"a\": \"test\", \"b\": 42.2, \"c\": [ {\"d\": 124, \"e\": \"Baz\"}, null, {\"d\": 457, \"e\": \"Quux\"} ]}",
                    "id", tableAndPublisher, subtableAndPublisher);
            Assert.fail("Should have thrown an exception");
        } catch (JSONIngesterException ex) {
            try {
                Assert.assertEquals("Failed processing subtable field \"c\"", ex.getMessage());
                Assert.assertEquals("Key 'd' not found in the record, but allowMissingKeys is false.",
                        ex.getCause().getMessage());
            } catch (AssertionError ae) {
                ex.printStackTrace();
                throw ae;
            }
        }
    }

    public void testRoutedTables() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factorySubtable_msgType1 = new JSONToStreamPublisherAdapterBuilder()
                .allowMissingKeys(false)
                .allowNullValues(false)
                .addColumnFromField("A", "a")
                .addColumnFromField("B", "b")
                .addColumnFromField("C", "c")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factorySubtable_msgType2 = new JSONToStreamPublisherAdapterBuilder()
                .allowMissingKeys(false)
                .allowNullValues(false)
                .addColumnFromField("A", "a")
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(false);

        final StreamPublisherAndTable msgType1TAP = createStreamPublisherAndTable(
                Arrays.asList("A", "B", "C", "SubtableRecordId"),
                Arrays.asList(String.class, double.class, int.class, long.class));

        final StreamPublisherAndTable msgType2TAP = createStreamPublisherAndTable(
                Arrays.asList("A", "D", "E", "SubtableRecordId"),
                Arrays.asList(String.class, String.class, int.class, long.class));

        final Table resultMsgType1 = msgType1TAP.getAppendOnlyTable();
        final Table resultMsgType2 = msgType2TAP.getAppendOnlyTable();

        final BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactoryWithSubtables(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .addColumnFromField("A", "a")
                                .addRoutedTableAdapter("msgType1", node -> node.get("MsgType").intValue() == 1,
                                        factorySubtable_msgType1)
                                .addRoutedTableAdapter("msgType2", node -> node.get("MsgType").intValue() == 2,
                                        factorySubtable_msgType2)
                                .autoValueMapping(false)
                                .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "msgType1_id", "msgType2_id");
        final List<Class<?>> types = Arrays.asList(String.class, long.class, long.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table resultMain = tableAndPublisher.getAppendOnlyTable();

        // noinspection RedundantTypeArguments (there's an unchecked assignment warning if the type args are removed)
        adapter = factory.apply(tableAndPublisher.getPublisher(),
                Map.<String, SimpleStreamPublisher>of(
                        "msgType1", msgType1TAP.getPublisher(),
                        "msgType2", msgType2TAP.getPublisher()));

        injectJson(
                "{\"MsgType\": 0, \"a\": \"test\"}",
                "msg0", tableAndPublisher, msgType1TAP, msgType2TAP);
        injectJson(
                "{\"MsgType\": 1, \"a\": \"test\", \"b\": 1.1, \"c\": 11}",
                "msg1", tableAndPublisher, msgType1TAP, msgType2TAP);
        injectJson(
                "{\"MsgType\": 2, \"a\": \"test\", \"b\": -1, \"d\": \"table2_row1\", \"e\": 21}",
                "msg2", tableAndPublisher, msgType1TAP, msgType2TAP);

        {
            // Check the main table:
            TableTools.show(resultMain);
            Assert.assertEquals(3, resultMain.intSize());
            final Table expectedMain = newTable(
                    col("A", "test", "test", "test"),
                    // the subtable row IDs
                    longCol("msgType1_id", NULL_LONG, 1, NULL_LONG),
                    longCol("msgType2_id", NULL_LONG, NULL_LONG, 2));
            Assert.assertEquals("", diff(resultMain, expectedMain, 10));

            // Check the subtable for message type 1:
            TableTools.show(resultMsgType1);
            Assert.assertEquals(1, resultMsgType1.intSize());
            final Table expectedSubtableMsgType1 = newTable(
                    col("A", "test"),
                    doubleCol("B", 1.1),
                    intCol("C", 11),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 1));
            Assert.assertEquals("", diff(resultMsgType1, expectedSubtableMsgType1, 10));

            // Check the subtable for message type 2:
            TableTools.show(resultMsgType2);
            Assert.assertEquals(1, resultMsgType2.intSize());
            final Table expectedSubtableMsgType2 = newTable(
                    col("A", "test"),
                    stringCol("D", "table2_row1"),
                    intCol("E", 21),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 2));
            Assert.assertEquals("", diff(resultMsgType2, expectedSubtableMsgType2, 10));
        }

        updateBlinkTables(tableAndPublisher, msgType1TAP, msgType2TAP);
    }

    public void testDoubleNestedRecord() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedGh = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("G", "g")
                .addColumnFromField("H", "h")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .addNestedField("f", factoryNestedGh)
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("I", "b")
                        .addColumnFromField("A", "a")
                        .addNestedFieldParallel("c", factoryNestedDe)
                        .autoValueMapping(false)
                        .sendTimestampColumnName("TM"));

        final List<String> names = Arrays.asList("A", "B", "D", "E", "G", "H", "I", "TM");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class, short.class,
                float.class, double.class, Instant.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"a\": \"test\", \"b\": 42.2, \"c\": [ {\"d\": 123, \"e\": \"Foo\", \"f\": {\"g\": 456, \"h\": 3.14 } }, {\"d\": 789, \"e\": \"Quux\", \"f\": {\"g\": 1011, \"h\": 2.71 } } ] }",
                "id", tableAndPublisher);

        final Table expected = newTable(
                col("A", "test", "test"),
                doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                longCol("D", 123, 789),
                stringCol("E", "Foo", "Quux"),
                shortCol("G", (short) 456, (short) 1011),
                floatCol("H", 3.14f, 2.71f),
                doubleCol("I", 42.2, 42.2),
                col("TM", DateTimeUtils.epochMillisToInstant(MESSAGE_TIMESTAMP_MILLIS),
                        DateTimeUtils.epochMillisToInstant(MESSAGE_TIMESTAMP_MILLIS)));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testTopLevelArray() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedGh = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("G", "g")
                .addColumnFromField("H", "h")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .addNestedField("f", factoryNestedGh)
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("I", "b")
                        .addColumnFromField("A", "a")
                        .addNestedFieldParallel("c", factoryNestedDe)
                        .autoValueMapping(false)
                        .processArrays(true));

        final List<String> names = Arrays.asList("A", "B", "D", "E", "G", "H", "I");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class, short.class,
                float.class, double.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "[{\"a\": \"test\", \"b\": 42.2, \"c\": [ {\"d\": 123, \"e\": \"Foo\", \"f\": {\"g\": 456, \"h\": 3.14 } }, {\"d\": 789, \"e\": \"Quux\", \"f\": {\"g\": 1011, \"h\": 2.71 } } ] },"
                        +
                        " {\"a\": \"test test\", \"b\": 47, \"c\": [ {\"d\": 1213, \"e\": \"Baz\", \"f\": {\"g\": 456, \"h\": 3.14 } }, {\"d\": 1415, \"e\": \"Fribble\", \"f\": {\"g\": 1011, \"h\": 2.71 } } ] }]",
                "id", tableAndPublisher);

        final Table expected = newTable(
                col("A", "test", "test", "test test", "test test"),
                doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE),
                longCol("D", 123, 789, 1213, 1415), stringCol("E", "Foo", "Quux", "Baz", "Fribble"),
                shortCol("G", (short) 456, (short) 1011, (short) 456, (short) 1011),
                floatCol("H", 3.14f, 2.71f, 3.14f, 2.71f), doubleCol("I", 42.2, 42.2, 47, 47));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testTopLevelSimpleArray() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromField("I", "b")
                        .addColumnFromField("A", "a")
                        .autoValueMapping(false)
                        .processArrays(true)
                        .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "I");
        final List<Class<?>> types = Arrays.asList(String.class, double.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("[{\"a\": \"test\", \"b\": 42.2 }, {\"a\": \"Bo\", \"b\": 21.1 }]", "id", tableAndPublisher);

        final Table expected = newTable(
                col("A", "test", "Bo"), doubleCol("I", 42.2, 21.1));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }


    public void testTopLevelSimpleArrayWithSubtable() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factorySubtableX = new JSONToStreamPublisherAdapterBuilder()
                .allowMissingKeys(true)
                .addColumnFromField("X", "x");

        final StreamPublisherAndTable subtableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList("X", "SubtableRecordId"),
                Arrays.asList(int.class, long.class));
        final Table resultSubtable = subtableAndPublisher.getAppendOnlyTable();


        final BiFunction<SimpleStreamPublisher, Map<String, SimpleStreamPublisher>, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactoryWithSubtables(log,
                        new JSONToStreamPublisherAdapterBuilder()
                                .addColumnFromField("A", "a")
                                .addColumnFromField("B", "b")
                                .addFieldToSubTableMapping("subtable", factorySubtableX)
                                .autoValueMapping(false)
                                .processArrays(true)
                                .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "B", "subtable_id");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table resultMain = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher(),
                Map.of("subtable", subtableAndPublisher.getPublisher()));

        injectJson(
                "[{\"a\": \"test\", \"b\": 42.2, \"subtable\": [{ \"x\": 42 }] }, {\"a\": \"test2\", \"b\": 21.1, \"subtable\": [{ \"x\": 43 }] }]",
                "id", tableAndPublisher, subtableAndPublisher);

        {
            // Check the main table:
            TableTools.show(resultMain);
            Assert.assertEquals(2, resultMain.intSize());
            final Table expectedMain = newTable(
                    col("A", "test", "test2"),
                    doubleCol("B", 42.2, 21.1),
                    // the subtable row IDs
                    longCol("subtable_id", 0, 1));
            Assert.assertEquals("", diff(resultMain, expectedMain, 10));

            // Check the subtable:
            TableTools.show(resultSubtable);
            Assert.assertEquals(2, resultSubtable.intSize());
            final Table expectedSubtable = newTable(
                    intCol("X", 42, 43),
                    // the nested parallel fields
                    longCol("SubtableRecordId", 0, 1));
            Assert.assertEquals("", diff(resultSubtable, expectedSubtable, 10));
        }

        updateBlinkTables(tableAndPublisher, subtableAndPublisher);
    }

    public void testMixedTopLevel() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedGh = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("G", "g")
                .addColumnFromField("H", "h")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .addNestedField("f", factoryNestedGh)
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("I", "b")
                        .addColumnFromField("A", "a")
                        .addNestedFieldParallel("c", factoryNestedDe)
                        .autoValueMapping(false)
                        .processArrays(true));

        final List<String> names = Arrays.asList("A", "B", "D", "E", "G", "H", "I");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class, short.class,
                float.class, double.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"a\": \"Singleton\", \"b\": 33.3, \"c\": [ {\"d\": 0, \"e\": \"Boom\", \"f\": {\"g\": 1, \"h\": 2.71 } }, {\"d\": 2, \"e\": \"Box\", \"f\": {\"g\": 3, \"h\": 1.41 } } ] }",
                "id", tableAndPublisher);

        injectJson(
                "[{\"a\": \"test\", \"b\": 42.2, \"c\": [ {\"d\": 123, \"e\": \"Foo\", \"f\": {\"g\": 456, \"h\": 3.14 } }, {\"d\": 789, \"e\": \"Quux\", \"f\": {\"g\": 1011, \"h\": 2.71 } } ] },"
                        +
                        " {\"a\": \"test test\", \"b\": 47, \"c\": [ {\"d\": 1213, \"e\": \"Baz\", \"f\": {\"g\": 456, \"h\": 3.14 } }, {\"d\": 1415, \"e\": \"Fribble\", \"f\": {\"g\": 1011, \"h\": 2.71 } } ] }]",
                "id", tableAndPublisher);

        final Table expected = newTable(
                col("A", "Singleton", "Singleton", "test", "test", "test test", "test test"),
                doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE,
                        QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                longCol("D", 0, 2, 123, 789, 1213, 1415),
                stringCol("E", "Boom", "Box", "Foo", "Quux", "Baz", "Fribble"),
                shortCol("G", (short) 1, (short) 3, (short) 456, (short) 1011, (short) 456, (short) 1011),
                floatCol("H", 2.71f, 1.41f, 3.14f, 2.71f, 3.14f, 2.71f),
                doubleCol("I", 33.3, 33.3, 42.2, 42.2, 47, 47));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testEmptyArrayOfMessages() throws InterruptedException, TimeoutException, IOException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("A", "a")
                        .processArrays(true));

        final List<String> names = Arrays.asList("A", "B", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        // First inject an empty message and verify nothing comes up as a result
        try {
            injectJson("[]", "id", tableAndPublisher);
        } catch (final TimeoutException te) {
            Assert.fail("Cleanup did not run properly!");
        }
        Assert.assertEquals(0, result.intSize());

        // Then inject a message with some content, and make sure it populates
        injectJson("{\"a\": \"test\", \"b\": 42.2, \"c\": 123}", "id", tableAndPublisher);
        Assert.assertEquals(1, result.intSize());

        final Table expected = newTable(col("A", "test"), doubleCol("B", QueryConstants.NULL_DOUBLE), intCol("c", 123));
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testNestedNulls() throws IOException, InterruptedException, TimeoutException {
        final JSONToStreamPublisherAdapterBuilder factoryNestedGh = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("G", "g")
                .addColumnFromField("H", "h")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .addNestedField("f", factoryNestedGh)
                .allowNullValues(true)
                .autoValueMapping(false);

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("I", "b")
                        .addColumnFromField("A", "a")
                        .addNestedField("c", factoryNestedDe)
                        .allowNullValues(true)
                        .autoValueMapping(false)
                        .nConsumerThreads(0));

        final List<String> names = Arrays.asList("A", "B", "D", "E", "G", "H", "I");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, long.class, String.class, short.class,
                float.class, double.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson(
                "{\"a\": \"Angelica\", \"b\": 42.2, \"c\": {\"d\": 123, \"e\": \"Foo\", \"f\": {\"g\": 456, \"h\": 3.14 } } }",
                "id", tableAndPublisher);

        final Table expected = newTable(
                col("A", "Angelica"), doubleCol("B", QueryConstants.NULL_DOUBLE),
                longCol("D", 123), stringCol("E", "Foo"), shortCol("G", (short) 456), floatCol("H", 3.14f),
                doubleCol("I", 42.2));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected, 10));

        injectJson("{\"a\": \"Eliza\", \"b\": 44, \"c\": {\"d\": 124, \"e\": \"Bar\", \"f\": null } }", "id",
                tableAndPublisher);

        final Table expected2 = newTable(
                col("A", "Angelica", "Eliza"), doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                longCol("D", 123, 124), stringCol("E", "Foo", "Bar"),
                shortCol("G", (short) 456, QueryConstants.NULL_SHORT), floatCol("H", 3.14f, QueryConstants.NULL_FLOAT),
                doubleCol("I", 42.2, 44));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected2, 10));

        injectJson("{\"a\": \"Peggy\", \"b\": 44, \"c\": null }", "id", tableAndPublisher);

        final Table expected3 = newTable(
                col("A", "Angelica", "Eliza", "Peggy"),
                doubleCol("B", QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE, QueryConstants.NULL_DOUBLE),
                longCol("D", 123, 124, NULL_LONG), stringCol("E", "Foo", "Bar", null),
                shortCol("G", (short) 456, QueryConstants.NULL_SHORT, QueryConstants.NULL_SHORT),
                floatCol("H", 3.14f, QueryConstants.NULL_FLOAT, QueryConstants.NULL_FLOAT),
                doubleCol("I", 42.2, 44, 44));
        TableTools.show(result);
        Assert.assertEquals("", diff(result, expected3, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testBadNesting() {
        final JSONToStreamPublisherAdapterBuilder factoryNestedDe = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .addFieldParallel("F", "f")
                .autoValueMapping(false);

        final JSONToStreamPublisherAdapterBuilder factoryNestedDe2 = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .autoValueMapping(true);

        final JSONToStreamPublisherAdapterBuilder factoryNestedDe3 = new JSONToStreamPublisherAdapterBuilder()
                .autoValueMapping(false)
                .addColumnFromField("D", "d")
                .addColumnFromField("E", "e")
                .allowUnmapped("F");

        try {
            new JSONToStreamPublisherAdapterBuilder()
                    .allowUnmapped("B")
                    .addColumnFromField("I", "b")
                    .addColumnFromField("A", "a")
                    .addNestedFieldParallel("c", factoryNestedDe)
                    .autoValueMapping(false)
                    .buildFactory(log);
            TestCase.fail("Expected an exception");
        } catch (final JSONIngesterException e) {
            TestCase.assertEquals("Nested fields may not contain parallel array fields, c!", e.getMessage());
        }

        try {
            new JSONToStreamPublisherAdapterBuilder()
                    .allowUnmapped("B")
                    .addColumnFromField("I", "b")
                    .addColumnFromField("A", "a")
                    .addNestedField("c", factoryNestedDe)
                    .autoValueMapping(false)
                    .buildFactory(log);
            TestCase.fail("Expected an exception");
        } catch (final JSONIngesterException e) {
            TestCase.assertEquals("Nested fields may not contain parallel array fields, c!", e.getMessage());
        }

        try {
            new JSONToStreamPublisherAdapterBuilder()
                    .addNestedField("c", factoryNestedDe2)
                    .buildFactory(log);
            TestCase.fail("Expected an exception");
        } catch (final JSONIngesterException e) {
            TestCase.assertEquals("Auto value mapping is not supported for nested field, c!", e.getMessage());
        }

        try {
            new JSONToStreamPublisherAdapterBuilder()
                    .addNestedField("c", factoryNestedDe3)
                    .buildFactory(log);
            TestCase.fail("Expected an exception");
        } catch (final JSONIngesterException e) {
            TestCase.assertEquals("Nested fields may not define unmapped fields, c!", e.getMessage());
        }

        try {
            new JSONToStreamPublisherAdapterBuilder()
                    .addNestedField("c", factoryNestedDe2.autoValueMapping(false).receiveTimestampColumnName("Foo"))
                    .buildFactory(log);
            TestCase.fail("Expected an exception");
        } catch (final JSONIngesterException e) {
            TestCase.assertEquals("Nested fields may not define message header columns field c, columns=[Foo]",
                    e.getMessage());
        }
    }

    public void testWaitForProcessingTimeout() throws IOException, InterruptedException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowUnmapped("B")
                        .addColumnFromField("A", "a")
                        .nConsumerThreads(-1));

        final List<String> names = Arrays.asList("A", "B", "c");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);

        adapter = factory.apply(tableAndPublisher.getPublisher());

        final StringMessageHolder msg = new StringMessageHolder("{\"a\": \"test\", \"b\": 42.2, \"c\": 123}");

        adapter.consumeMessage("id", msg);

        try {
            // this will unfortunately take MAX_WAIT_MILLIS to finish
            adapter.waitForProcessing(10_000L);
            Assert.fail("Expected timeout exception did not occur");
        } catch (final TimeoutException ex) {
            // expected;
        }
    }

    public void testMissingColumns() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .autoValueMapping(false)
                        .allowUnmapped("unmapped")
                        .addColumnFromField("str", "a")
                        .addColumnFromDoubleFunction("dbl", (x) -> 1.5)
                        .addColumnFromIntFunction("iger", (x) -> 2)
                        .addColumnFromLongFunction("lng", (x) -> (long) 3.0)
                        .addColumnFromFunction("obj", Object.class, (x) -> new Object()));

        final List<String> names = Arrays.asList("str", "dbl", "iger", "lng", "obj", "unmapped", "needmap");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class, long.class, Object.class,
                Object.class, Object.class);

        try (final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types)) {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should have failed to consume message");
        } catch (final JSONIngesterException jie) {
            Assert.assertEquals(
                    "Found columns without mappings [needmap], allowed unmapped=[unmapped], mapped to fields=[str], mapped to int functions=[iger], mapped to long functions=[lng], mapped to double functions=[dbl], mapped to functions=[obj]",
                    jie.getMessage());
        }
    }

    public void testColumnTypeMismatchDouble() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .autoValueMapping(false)
                        .allowUnmapped("unmapped")
                        .addColumnFromField("str", "a")
                        .addColumnFromDoubleFunction("dbl", (x) -> 1.5)
                        .addColumnFromIntFunction("iger", (x) -> 2)
                        .addColumnFromLongFunction("lng", (x) -> (long) 3.0)
                        .addColumnFromFunction("obj", String.class, (x) -> ""));

        final List<String> names = Arrays.asList("str", "dbl", "iger", "lng", "obj", "unmapped");
        final List<Class<?>> types =
                Arrays.asList(String.class, int.class, double.class, int.class, int.class, Object.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        try {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should have failed to consume message");
        } catch (final JSONIngesterException jie) {
            Assert.assertEquals("Column dbl is of type int, can not assign ToDoubleFunction.", jie.getMessage());
        }
    }

    public void testColumnTypeMismatchInt() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .autoValueMapping(false)
                        .allowUnmapped("unmapped")
                        .addColumnFromField("str", "a")
                        .addColumnFromDoubleFunction("dbl", (x) -> 1.5)
                        .addColumnFromIntFunction("iger", (x) -> 2)
                        .addColumnFromLongFunction("lng", (x) -> (long) 3.0)
                        .addColumnFromFunction("obj", String.class, (x) -> ""));

        final List<String> names = Arrays.asList("str", "dbl", "iger", "lng", "obj", "unmapped");
        final List<Class<?>> types =
                Arrays.asList(String.class, double.class, double.class, int.class, int.class, Object.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        try {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should have failed to consume message");
        } catch (final JSONIngesterException jie) {
            Assert.assertEquals("Column iger is of type double, can not assign ToIntFunction.", jie.getMessage());
        }
    }

    public void testColumnTypeMismatchLong() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .autoValueMapping(false)
                        .allowUnmapped("unmapped")
                        .addColumnFromField("str", "a")
                        .addColumnFromDoubleFunction("dbl", (x) -> 1.5)
                        .addColumnFromIntFunction("iger", (x) -> 2)
                        .addColumnFromLongFunction("lng", (x) -> (long) 3.0)
                        .addColumnFromFunction("obj", String.class, (x) -> ""));

        final List<String> names = Arrays.asList("str", "dbl", "iger", "lng", "obj", "unmapped");
        final List<Class<?>> types =
                Arrays.asList(String.class, double.class, int.class, int.class, int.class, Object.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        try {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should have failed to consume message");
        } catch (final JSONIngesterException jie) {
            Assert.assertEquals("Column lng is of type int, can not assign ToLongFunction.", jie.getMessage());
        }
    }

    public void testFunction() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromFunction("StrCol", String.class,
                                (r) -> JsonNodeUtil.getString(r, "a", false, false).split(",")[0])
                        .addColumnFromIntFunction("IntCol", (r) -> JsonNodeUtil.getInt(r, "c", false, false) * 2)
                        .addColumnFromLongFunction("LongCol",
                                (r) -> JsonNodeUtil.getLong(r, "c", false, false) * (1L << 32))
                        .addColumnFromDoubleFunction("DoubleCol", (r) -> {
                            double sum = 0;
                            for (final JsonNode node : r.get("b")) {
                                sum += node.asDouble();
                            }
                            return sum;
                        }));

        final List<String> names = Arrays.asList("StrCol", "DoubleCol", "IntCol", "LongCol");
        final List<Class<?>> types = Arrays.asList(String.class, double.class, int.class, long.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"Hello, world\", \"b\": [42.2, 37], \"c\": 123}", "id", tableAndPublisher);

        final Table expected = newTable(col("StrCol", "Hello"),
                doubleCol("DoubleCol", 79.2),
                intCol("IntCol", 246),
                longCol("LongCol", 123L << 32));
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }

    public void testFunctionWithMismatchedColumnType() {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromFunction("StrCol", Object.class, (r) -> new Object()));

        final List<String> names = Arrays.asList("StrCol");
        final List<Class<?>> types = Arrays.asList(String.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);

        try {
            factory.apply(tableAndPublisher.getPublisher());
            Assert.fail("Should have failed to apply factory!");
        } catch (final JSONIngesterException jie) {
            Assert.assertEquals(
                    "Column StrCol is of type class java.lang.String, can not assign function of type: class java.lang.Object",
                    jie.getMessage());
        }
    }

    public void testTypedGenericColumnSetterFunctions() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .addColumnFromFunction("StrCol", String.class,
                                (r) -> JsonNodeUtil.getString(r, "a", false, false).split(",")[0])
                        .addColumnFromFunction("IntCol", int.class,
                                (r) -> JsonNodeUtil.getInt(r, "c", false, false) * 2)
                        .addColumnFromFunction("LongCol", long.class,
                                (r) -> JsonNodeUtil.getLong(r, "c", false, false) * (1L << 32))
                        .addColumnFromFunction("ShortCol", short.class,
                                (r) -> JsonNodeUtil.getShort(r, "c", false, false))
                        .addColumnFromFunction("FloatCol", float.class,
                                (r) -> JsonNodeUtil.getFloat(r, "c", false, false) * 0.5f)
                        .addColumnFromFunction("ByteCol", byte.class, (r) -> (byte) 3)
                        .addColumnFromFunction("DoubleCol", double.class, (r) -> {
                            double sum = 0;
                            for (final JsonNode node : r.get("b")) {
                                sum += node.asDouble();
                            }
                            return sum;
                        })
                        .addColumnFromFunction("BoolCol", Boolean.class,
                                (r) -> JsonNodeUtil.getChar(r, "d", false, false) == 'a')
                        .addColumnFromFunction("CharCol", char.class, (r) -> JsonNodeUtil.getChar(r, "d", false, false))
                        .nConsumerThreads(0));

        final List<String> names = Arrays.asList("StrCol", "DoubleCol", "IntCol", "LongCol", "ShortCol", "FloatCol",
                "ByteCol", "BoolCol", "CharCol");
        final List<Class<?>> types =
                Arrays.asList(String.class, double.class, int.class, long.class, short.class, float.class,
                        byte.class, Boolean.class, char.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        injectJson("{\"a\": \"Hello, world\", \"b\": [42.2, 37], \"c\": 123, \"d\": \"a\"}", "id", tableAndPublisher);

        final Table expected = newTable(col("StrCol", "Hello"),
                doubleCol("DoubleCol", 79.2),
                intCol("IntCol", 246),
                longCol("LongCol", 123L << 32),
                shortCol("ShortCol", (short) 123),
                floatCol("FloatCol", 61.5f),
                byteCol("ByteCol", (byte) 3),
                col("BoolCol", true),
                col("CharCol", 'a'));
        Assert.assertEquals("", diff(result, expected, 10));

        updateBlinkTables(tableAndPublisher);
    }


    /**
     * Run the {@code jsonStr} through the adapter and refresh the {@code tablesToRefresh}.
     *
     * @param jsonStr The JSON string to wrap in a message and send to the adapter
     * @param msgId The message ID string
     * @param streamToBlinkAdapters StreamToBlinkTableAdapters to {@link StreamToBlinkTableAdapter#run() refresh} after
     *        consuming and publishing the message
     */
    private void injectJson(final String jsonStr, final String msgId,
            final StreamPublisherAndTable... streamToBlinkAdapters)
            throws IOException, InterruptedException, TimeoutException {
        injectJson(
                jsonStr,
                msgId,
                Arrays.stream(streamToBlinkAdapters)
                        .map(StreamPublisherAndTable::getAdapter)
                        .toArray(StreamToBlinkTableAdapter[]::new));
    }

    /**
     * Run the {@code jsonStr} through the adapter and refresh the {@code tablesToRefresh}.
     *
     * @param jsonStr The JSON string to wrap in a message and send to the adapter
     * @param msgId The message ID string
     * @param streamToBlinkAdapters StreamToBlinkTableAdapters to {@link StreamToBlinkTableAdapter#run() refresh} after
     *        consuming and publishing the message
     */
    private void injectJson(final String jsonStr, final String msgId,
            final StreamToBlinkTableAdapter... streamToBlinkAdapters)
            throws IOException, InterruptedException, TimeoutException {
        final StringMessageHolder msg = new StringMessageHolder(MESSAGE_TIMESTAMP_MILLIS * 1000L, jsonStr);

        adapter.consumeMessage(msgId, msg);

        adapter.waitForProcessing(MAX_WAIT_MILLIS);
        adapter.cleanup();
        updateBlinkTables(streamToBlinkAdapters);
    }

    private static void updateBlinkTables(StreamPublisherAndTable... streamToBlinkAdapters) {
        updateBlinkTables(
                Arrays.stream(streamToBlinkAdapters)
                        .map(StreamPublisherAndTable::getAdapter)
                        .toArray(StreamToBlinkTableAdapter[]::new));
    }


    /**
     * Update the output blink tables byc calling {@link StreamToBlinkTableAdapter#run() run()} on the adapters, during
     * the UpdateGraph's unit test cycle.
     * <p>
     * This also closes the chunks that were handed off to the blink table on the previous cycle. Accordingly, this must
     * be called for each adapter at the end of each test.
     *
     * @param streamToBlinkAdapters The adapters to update
     */
    private static void updateBlinkTables(StreamToBlinkTableAdapter... streamToBlinkAdapters) {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        updateGraph.runWithinUnitTestCycle(() -> {
            System.out.println("Running cycle.");
            for (StreamToBlinkTableAdapter streamToBlinkAdapter : streamToBlinkAdapters) {
                streamToBlinkAdapter.run();
            }
        });
    }

    private static JsonStreamAdapterTest.StreamPublisherAndTable createStreamPublisherAndTable(
            final List<String> colNames,
            final List<Class<?>> colTypes) {
        final SimpleStreamPublisher publisher = new SimpleStreamPublisher(TableDefinition.of(
                colNames,
                colTypes));

        final StreamToBlinkTableAdapter subtableStreamToBlinkTableAdapter = new StreamToBlinkTableAdapter(
                publisher.getTableDefinition(),
                publisher,
                ExecutionContext.getContext().getUpdateGraph(),
                "streamToBlinkAdapter");
        return new JsonStreamAdapterTest.StreamPublisherAndTable(publisher, subtableStreamToBlinkTableAdapter);
    }

    private static class StreamPublisherAndTable implements AutoCloseable {
        private final SimpleStreamPublisher publisher;
        private final StreamToBlinkTableAdapter adapter;
        private final Table appendOnlyTable;

        public StreamPublisherAndTable(SimpleStreamPublisher publisher, StreamToBlinkTableAdapter adapter) {
            this.publisher = publisher;
            this.adapter = adapter;
            appendOnlyTable = BlinkTableTools.blinkToAppendOnly(adapter.table());
        }

        public SimpleStreamPublisher getPublisher() {
            return publisher;
        }

        public StreamToBlinkTableAdapter getAdapter() {
            return adapter;
        }

        public Table getBlinkTable() {
            return adapter.table();
        }

        public Table getAppendOnlyTable() {
            return appendOnlyTable;
        }

        @Override
        public void close() {
            adapter.close();
            appendOnlyTable.close();
        }
    }

    public void testDuplicateColumns() {
        final JSONToStreamPublisherAdapterBuilder builder = new JSONToStreamPublisherAdapterBuilder();
        builder.allowUnmapped("A");
        expectAlreadyDefined(() -> builder.addColumnFromField("A", "B"));
        expectAlreadyDefined(() -> builder.addColumnFromIntFunction("A", (r) -> QueryConstants.NULL_INT));
        expectAlreadyDefined(() -> builder.addColumnFromLongFunction("A", (r) -> NULL_LONG));
        expectAlreadyDefined(() -> builder.addColumnFromDoubleFunction("A", (r) -> QueryConstants.NULL_DOUBLE));
        expectAlreadyDefined(() -> builder.addColumnFromFunction("A", String.class, (r) -> "test"));

        final JSONToStreamPublisherAdapterBuilder builder2 = new JSONToStreamPublisherAdapterBuilder();
        builder2.addColumnFromField("A", "B");
        expectAlreadyDefined(() -> builder2.allowUnmapped("A"));
        expectAlreadyDefined(() -> builder2.addColumnFromIntFunction("A", (r) -> QueryConstants.NULL_INT));
        expectAlreadyDefined(() -> builder2.addColumnFromLongFunction("A", (r) -> NULL_LONG));
        expectAlreadyDefined(() -> builder2.addColumnFromDoubleFunction("A", (r) -> QueryConstants.NULL_DOUBLE));
        expectAlreadyDefined(() -> builder2.addColumnFromFunction("A", String.class, (r) -> "test"));

        final JSONToStreamPublisherAdapterBuilder builder3 = new JSONToStreamPublisherAdapterBuilder();
        builder3.addColumnFromIntFunction("A", (r) -> QueryConstants.NULL_INT);
        expectAlreadyDefined(() -> builder3.allowUnmapped("A"));
        expectAlreadyDefined(() -> builder3.addColumnFromField("A", "B"));
        expectAlreadyDefined(() -> builder3.addColumnFromDoubleFunction("A", (r) -> QueryConstants.NULL_DOUBLE));
        expectAlreadyDefined(() -> builder3.addColumnFromLongFunction("A", (r) -> QueryConstants.NULL_INT));
        expectAlreadyDefined(() -> builder3.addColumnFromFunction("A", String.class, (r) -> "test"));

        final JSONToStreamPublisherAdapterBuilder builder4 = new JSONToStreamPublisherAdapterBuilder();
        builder4.addColumnFromLongFunction("A", (r) -> NULL_LONG);
        expectAlreadyDefined(() -> builder4.allowUnmapped("A"));
        expectAlreadyDefined(() -> builder4.addColumnFromField("A", "B"));
        expectAlreadyDefined(() -> builder4.addColumnFromIntFunction("A", (r) -> QueryConstants.NULL_INT));
        expectAlreadyDefined(() -> builder4.addColumnFromDoubleFunction("A", (r) -> QueryConstants.NULL_DOUBLE));
        expectAlreadyDefined(() -> builder4.addColumnFromFunction("A", String.class, (r) -> "test"));

        final JSONToStreamPublisherAdapterBuilder builder5 = new JSONToStreamPublisherAdapterBuilder();
        builder5.addColumnFromDoubleFunction("A", (r) -> QueryConstants.NULL_DOUBLE);
        expectAlreadyDefined(() -> builder5.allowUnmapped("A"));
        expectAlreadyDefined(() -> builder5.addColumnFromField("A", "B"));
        expectAlreadyDefined(() -> builder5.addColumnFromIntFunction("A", (r) -> QueryConstants.NULL_INT));
        expectAlreadyDefined(() -> builder5.addColumnFromLongFunction("A", (r) -> NULL_LONG));
        expectAlreadyDefined(() -> builder5.addColumnFromFunction("A", String.class, (r) -> "test"));

        final JSONToStreamPublisherAdapterBuilder builder6 = new JSONToStreamPublisherAdapterBuilder();
        builder6.addColumnFromFunction("A", String.class, (r) -> "test");
        expectAlreadyDefined(() -> builder6.allowUnmapped("A"));
        expectAlreadyDefined(() -> builder6.addColumnFromField("A", "B"));
        expectAlreadyDefined(() -> builder6.addColumnFromIntFunction("A", (r) -> QueryConstants.NULL_INT));
        expectAlreadyDefined(() -> builder6.addColumnFromLongFunction("A", (r) -> NULL_LONG));
        expectAlreadyDefined(() -> builder4.addColumnFromDoubleFunction("A", (r) -> QueryConstants.NULL_DOUBLE));
    }

    private void expectAlreadyDefined(final Runnable r) {
        try {
            r.run();
            TestCase.fail();
        } catch (final JSONIngesterException e) {
            Assert.assertTrue(e.getMessage().startsWith("Column \"A\" is already defined: "));
        }
    }

    public void testPerformanceSmallMessages() throws IOException, InterruptedException, TimeoutException {
        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder());

        final String strCol = "str";
        final String dblCol = "dbl";
        final String intCol = "iger";
        final String shortCol = "shrt";
        final String longCol = "lng";
        final String byteCol = "byt";
        final String floatCol = "flt";

        final String[] names = new String[7];
        final Class<?>[] types = new Class[names.length];
        final int groupSize = names.length / 7;
        for (int i = 0; i < names.length; i++) {

            if (i < groupSize) {
                // noinspection ConstantConditions
                names[i] = strCol + (i % groupSize);
                types[i] = String.class;
            } else if (i < 2 * groupSize) {
                // noinspection ConstantConditions
                names[i] = dblCol + (i % groupSize);
                types[i] = double.class;
            } else if (i < 3 * groupSize) {
                // noinspection ConstantConditions
                names[i] = intCol + (i % groupSize);
                types[i] = int.class;
            } else if (i < 4 * groupSize) {
                // noinspection ConstantConditions
                names[i] = shortCol + (i % groupSize);
                types[i] = short.class;
            } else if (i < 5 * groupSize) {
                // noinspection ConstantConditions
                names[i] = longCol + (i % groupSize);
                types[i] = long.class;
            } else if (i < 6 * groupSize) {
                // noinspection ConstantConditions
                names[i] = byteCol + (i % groupSize);
                types[i] = byte.class;
            } else {
                // noinspection ConstantConditions
                names[i] = floatCol + (i % groupSize);
                types[i] = float.class;
            }
        }

        final StringMessageHolder[] messages = new StringMessageHolder[200000];

        final int mgGroupSize = 1;
        for (int i = 0; i < messages.length; i++) {
            final StringBuilder builder = new StringBuilder("{" + System.lineSeparator());
            for (int mg = 0; mg < 7; mg++) {
                if (mg < mgGroupSize) {
                    // noinspection ConstantConditions
                    builder.append("\"" + strCol).append(mg % mgGroupSize).append("\": \"test\",")
                            .append(System.lineSeparator());
                } else if (mg < 2 * mgGroupSize) {
                    // noinspection ConstantConditions
                    builder.append("\"" + dblCol).append(mg % mgGroupSize).append("\": ").append(mg + 1).append(".2,")
                            .append(System.lineSeparator());
                } else if (mg < 3 * mgGroupSize) {
                    // noinspection ConstantConditions
                    builder.append("\"" + intCol).append(mg % mgGroupSize).append("\": ").append(mg + 1).append("23,")
                            .append(System.lineSeparator());
                } else if (mg < 4 * mgGroupSize) {
                    // noinspection ConstantConditions
                    builder.append("\"" + shortCol).append(mg % mgGroupSize).append("\": ").append(mg + 1).append(",")
                            .append(System.lineSeparator());
                } else if (mg < 5 * mgGroupSize) {
                    // noinspection ConstantConditions
                    builder.append("\"" + longCol).append(mg % mgGroupSize).append("\": ").append(mg + 1)
                            .append("23456789,").append(System.lineSeparator());
                } else if (mg < 6 * mgGroupSize) {
                    // noinspection ConstantConditions
                    builder.append("\"" + byteCol).append(mg % mgGroupSize).append("\": 3,")
                            .append(System.lineSeparator());
                } else {
                    // noinspection ConstantConditions
                    builder.append("\"" + floatCol).append(mg % mgGroupSize).append("\": 98765.4321,")
                            .append(System.lineSeparator());
                }
            }
            builder.deleteCharAt(builder.length() - 2); // Remove trailing comma
            builder.append("}");

            messages[i] = new StringMessageHolder(builder.toString());
        }

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList(names),
                Arrays.asList(types));
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        // Give time for the adapter to get set up, including a few initial messages so the setters exist on each thread
        adapter.consumeMessage("id", messages[0]);
        adapter.consumeMessage("id", messages[1]);
        adapter.consumeMessage("id", messages[2]);
        adapter.consumeMessage("id", messages[3]);
        adapter.waitForProcessing(MAX_WAIT_MILLIS);

        long before = System.nanoTime();
        for (int i = 4; i < messages.length; i++) {
            adapter.consumeMessage("id", messages[i]);
        }
        long after = System.nanoTime();
        long intervalNanos = after - before;
        System.out.println("Consumed " + messages.length + " in " + intervalNanos / 1_000_000L + "ms, "
                + NANOS_PER_SECOND * messages.length / intervalNanos + " msgs/sec");
        before = System.nanoTime();
        while (result.intSize() < messages.length) {
            adapter.cleanup();

            updateBlinkTables(tableAndPublisher);

            // busy-wait or else the processor threads will pause too
            // noinspection StatementWithEmptyBody
            while (System.nanoTime() - before < 100_000_000L) {
                // Do nothing
            }
        }
        after = System.nanoTime();
        intervalNanos = after - before;
        System.out.println("Processed " + result.intSize() + " in " + intervalNanos / 1_000_000L + "ms, "
                + NANOS_PER_SECOND * messages.length / intervalNanos + " msgs/sec");

        Assert.assertEquals(messages.length, result.intSize());
        // Somewhat arbitrarily picking 50,000 messages per second as a minimum performance benchmark.
        Assert.assertTrue("Performance minimum", NANOS_PER_SECOND * messages.length / intervalNanos > 50000);

        updateBlinkTables(tableAndPublisher);
    }

    public void testPerformanceBigMessages() throws IOException, InterruptedException, TimeoutException {
        // Many other tests want fewer threads. For the performance test, we want to standardize on 4.
        final int numThreads = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() - 1));

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .nConsumerThreads(numThreads));

        final String strCol = "str";
        final String dblCol = "dbl";
        final String intCol = "iger";
        final String shortCol = "shrt";
        final String longCol = "lng";
        final String byteCol = "byt";
        final String floatCol = "flt";

        final String[] names = new String[140];
        final Class<?>[] types = new Class[names.length];
        final int groupSize = names.length / 7;
        for (int i = 0; i < names.length; i++) {

            if (i < groupSize) {
                names[i] = strCol + (i % groupSize);
                types[i] = String.class;
            } else if (i < 2 * groupSize) {
                names[i] = dblCol + (i % groupSize);
                types[i] = double.class;
            } else if (i < 3 * groupSize) {
                names[i] = intCol + (i % groupSize);
                types[i] = int.class;
            } else if (i < 4 * groupSize) {
                names[i] = shortCol + (i % groupSize);
                types[i] = short.class;
            } else if (i < 5 * groupSize) {
                names[i] = longCol + (i % groupSize);
                types[i] = long.class;
            } else if (i < 6 * groupSize) {
                names[i] = byteCol + (i % groupSize);
                types[i] = byte.class;
            } else {
                names[i] = floatCol + (i % groupSize);
                types[i] = float.class;
            }
        }

        final int warmupMessages = 20_000;

        final int numMessages = numThreads * 25_000;
        final StringMessageHolder[] messages = new StringMessageHolder[warmupMessages + numMessages];

        System.out.println("Generating test messages...");
        final int mgGroupSize = 30;
        final StringBuilder builder = new StringBuilder(1024);
        for (int i = 0; i < messages.length; i++) {
            builder.setLength(0);
            builder.append('{').append(System.lineSeparator());
            for (int mg = 0; mg < 210; mg++) {
                if (mg < mgGroupSize) {
                    builder.append("\"" + strCol).append(mg % mgGroupSize).append("\": \"test").append(i).append("\",")
                            .append(System.lineSeparator());
                } else if (mg < 2 * mgGroupSize) {
                    builder.append("\"" + dblCol).append(mg % mgGroupSize).append("\": ").append(Random.random())
                            .append(',')
                            .append(System.lineSeparator());
                } else if (mg < 3 * mgGroupSize) {
                    builder.append("\"" + intCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomInt(0, Integer.MAX_VALUE)).append(',')
                            .append(System.lineSeparator());
                } else if (mg < 4 * mgGroupSize) {
                    builder.append("\"" + shortCol).append(mg % mgGroupSize).append("\": ").append(mg + 1).append(',')
                            .append(System.lineSeparator());
                } else if (mg < 5 * mgGroupSize) {
                    builder.append("\"" + longCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomLong(-Long.MAX_VALUE, Long.MAX_VALUE))
                            .append(',').append(System.lineSeparator());
                } else if (mg < 6 * mgGroupSize) {
                    builder.append("\"" + byteCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomInt(Byte.MIN_VALUE, Byte.MAX_VALUE + 1)).append(',')
                            .append(System.lineSeparator());
                } else {
                    builder.append("\"" + floatCol).append(mg % mgGroupSize).append("\": ")
                            .append(Random.randomFloat(-10000, 10000)).append(',')
                            .append(System.lineSeparator());
                }
            }
            builder.deleteCharAt(builder.length() - 2); // Remove trailing comma
            builder.append('}');

            messages[i] = new StringMessageHolder(builder.toString());
        }

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList(names),
                Arrays.asList(types));
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        int msgIdx = 0;

        System.out.println("Warming up...");
        // Give time for the adapter to get set up, including a few initial messages so the setters exist on each thread
        for (; msgIdx < warmupMessages; msgIdx++) {
            adapter.consumeMessage("id", messages[msgIdx]);
        }

        adapter.waitForProcessing(MAX_WAIT_MILLIS);
        adapter.cleanup();

        System.out.println("Running test...");
        long beforeConsume = System.nanoTime();
        for (; msgIdx < messages.length; msgIdx++) {
            adapter.consumeMessage("id", messages[msgIdx]);
        }
        adapter.waitForProcessing(MAX_WAIT_MILLIS);
        long afterConsume = System.nanoTime();
        long intervalNanosConsume = afterConsume - beforeConsume;
        System.out.println("Consumed " + numMessages + " in " + intervalNanosConsume / 1_000_000L + "ms, "
                + NANOS_PER_SECOND * numMessages / intervalNanosConsume + " msgs/sec");


        long beforeCleanup = System.nanoTime();
        while (result.intSize() < messages.length) {
            adapter.cleanup();

            updateBlinkTables(tableAndPublisher);

            // busy-wait or else the processor threads will pause too
            // noinspection StatementWithEmptyBody
            while (System.nanoTime() - beforeCleanup < 200_000_000L) {
                // Do nothing
            }
        }
        long afterCleanup = System.nanoTime();
        long intervalNanosCleanup = afterCleanup - beforeCleanup;
        System.out.println("Processed " + numMessages + " in " + intervalNanosCleanup / 1_000_000L + "ms, "
                + NANOS_PER_SECOND * numMessages / intervalNanosCleanup + " msgs/sec");

        // Somewhat arbitrarily picking 30,000 messages per second as a minimum performance benchmark
        // - note that this test is lumping the LTM in with the imports, so actual performance should be higher.
        Assert.assertEquals(messages.length, result.intSize());


        // log consumeMessage() performance

        long minConsumedMessagesPerSecEachThread = 4_000L;
        long expectedConsumedMsgsPerSecOverall = minConsumedMessagesPerSecEachThread * numThreads;
        long realizedConsumedMsgsPerSec = Math.round(numMessages / (intervalNanosConsume / NANOS_PER_SECOND));

        System.out.println(
                "realizedConsumedMsgsPerSec=" + realizedConsumedMsgsPerSec + "; expectedConsumedMsgsPerSecOverall="
                        + expectedConsumedMsgsPerSecOverall);

        // log cleanup() performance

        long minCleanupMessagesPerSecEachThread = 25_000L;
        long expectedCleanupMsgsPerSecOverall = minCleanupMessagesPerSecEachThread * numThreads;
        long realizedCleanupMsgsPerSec = Math.round(numMessages / (intervalNanosCleanup / NANOS_PER_SECOND));

        System.out.println(
                "realizedConsumedMsgsPerSec=" + realizedCleanupMsgsPerSec + "; expectedConsumedMsgsPerSecOverall="
                        + expectedCleanupMsgsPerSecOverall);

        // check consumeMessage() performance
        Assert.assertTrue(
                "expected realizedConsumedMsgsPerSec > expectedConsumedMsgsPerSecOverall; realizedConsumedMsgsPerSec="
                        + realizedConsumedMsgsPerSec
                        + ", expectedConsumedMsgsPerSecOverall=" + expectedConsumedMsgsPerSecOverall,
                realizedConsumedMsgsPerSec > expectedConsumedMsgsPerSecOverall);

        // check cleanup() performance
        Assert.assertTrue(
                "expected realizedConsumedMsgsPerSec > expectedConsumedMsgsPerSecOverall; realizedConsumedMsgsPerSec="
                        + realizedCleanupMsgsPerSec
                        + ", expectedConsumedMsgsPerSecOverall=" + expectedCleanupMsgsPerSecOverall,
                realizedCleanupMsgsPerSec > expectedCleanupMsgsPerSecOverall);

        updateBlinkTables(tableAndPublisher);
    }

    public void testBadParse() throws IOException, InterruptedException, TimeoutException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Logger log = new StreamLoggerImpl(baos, LogLevel.ERROR);

        // noinspection Convert2Lambda
        ProcessEnvironment.getGlobalFatalErrorReporter().addInterceptor(new FatalErrorReporter.Interceptor() {
            @Override
            public void intercept(@NotNull final String message, @NotNull final Throwable throwable,
                    boolean isFromUncaught) {
                System.out.println("Uncaught exception: " + throwable.getMessage());
                throwable.printStackTrace();

                // throwing this exception will crash FatalErrorReporterBase.report() before it has a chance to call
                // System.exit().
                throw new RuntimeException(throwable);
            }
        });

        final Function<SimpleStreamPublisher, StringMessageToBlinkTableAdapter<StringMessageHolder>> factory =
                StringMessageToBlinkTableAdapter.buildFactory(log, new JSONToStreamPublisherAdapterBuilder()
                        .allowNullValues(true)
                        .nConsumerThreads(1));

        final String strCol = "str";
        final String boolColName = "bln";

        final List<String> names = Arrays.asList(strCol, boolColName);
        final List<Class<?>> types = Arrays.asList(String.class, Boolean.class);

        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(names, types);
        final Table result = tableAndPublisher.getAppendOnlyTable();

        adapter = factory.apply(tableAndPublisher.getPublisher());

        final StringMessageHolder msg = new StringMessageHolder("{\"" + strCol + "\": \"test\", \""
                + boolColName + "\": null "
                + "}");

        adapter.consumeMessage("id", msg);

        // Because the message will be consumed almost instantly, then actually processed separately, we have to wait to
        // see the results.
        adapter.waitForProcessing(60_000);
        adapter.cleanup();

        updateBlinkTables(tableAndPublisher);

        final Table expected = newTable(col(strCol, "test"),
                ColumnHolder.getBooleanColumnHolder(boolColName, false, (byte) 2));

        Assert.assertEquals("", diff(result, expected, 10));

        Assert.assertEquals("", baos.toString(StandardCharsets.UTF_8));
        baos.reset();

        final StringMessageHolder msg2 = new StringMessageHolder("~x=y;b=c");
        adapter.consumeMessage("id", msg2);

        final StringMessageHolder msg3 = new StringMessageHolder("{\"" + strCol + "\": \"Yikes\", \""
                + boolColName + "\": false "
                + "}");
        adapter.consumeMessage("id", msg3);

        // Because the message will be consumed almost instantly, then actually processed separately, we have to wait to
        // see the results.
        adapter.waitForProcessing(1000);
        adapter.cleanup();

        updateBlinkTables(tableAndPublisher);

        final Table expected3 = newTable(col(strCol, "test", "Yikes"),
                ColumnHolder.getBooleanColumnHolder(boolColName, false, (byte) 2, (byte) 0));

        Assert.assertEquals("", diff(result, expected3, 10));

        final String logText = baos.toString(StandardCharsets.UTF_8);
        if (!logText.startsWith(
                "Unable to parse JSON message #1: \"~x=y;b=c\": \nio.deephaven.jsoningester.JsonNodeUtil$JsonStringParseException: Failed to parse JSON string.")) {
            TestCase.fail("Expected JSON parse error in log, but was : " + logText);
        }
        baos.reset();

        updateBlinkTables(tableAndPublisher);
    }

    /**
     * Test an adapter built directly (rather than wrapped by {@link StringMessageToBlinkTableAdapter#buildFactory}). In
     * this case there is no {@link MessageMetadata} to process.
     */
    public void testNoMessageAdapter() throws IOException {
        final StreamPublisherAndTable tableAndPublisher = createStreamPublisherAndTable(
                Arrays.asList("Col1"),
                Arrays.asList(String.class));

        final JSONToStreamPublisherAdapter adapter = new JSONToStreamPublisherAdapterBuilder()
                .addColumnFromField("Col1", "field1")
                .nConsumerThreads(0)
                .makeAdapter(log, tableAndPublisher.getPublisher());

        adapter.consumeString("{ \"field1\": \"hello\"}");

        adapter.cleanup();

        final Table table = tableAndPublisher.getAppendOnlyTable();

        updateBlinkTables(tableAndPublisher);

        Assert.assertEquals("table.size()", 1, table.size());

        String value = (String) table.getColumnSource("Col1").get(0);
        Assert.assertEquals("hello", value);

        updateBlinkTables(tableAndPublisher);
    }

}
