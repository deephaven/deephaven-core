//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.json.ArrayValue;
import io.deephaven.json.ByteValue;
import io.deephaven.json.CharValue;
import io.deephaven.json.DoubleValue;
import io.deephaven.json.FloatValue;
import io.deephaven.json.IntValue;
import io.deephaven.json.LongValue;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.ShortValue;
import io.deephaven.json.StringValue;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.kafka.testcontainers.KafkaService;
import io.deephaven.qst.type.Type;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.VoidSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.deephaven.engine.table.ColumnDefinition.ofInt;
import static io.deephaven.engine.table.ColumnDefinition.ofLong;
import static io.deephaven.engine.table.ColumnDefinition.ofString;
import static io.deephaven.engine.table.ColumnDefinition.ofTime;
import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.kafka.KafkaTools.ALL_PARTITIONS;
import static io.deephaven.kafka.KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING;
import static io.deephaven.kafka.KafkaTools.KAFKA_PARTITION_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.OFFSET_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.TIMESTAMP_COLUMN_NAME_DEFAULT;

@Tag("testcontainers")
class KafkaToolsIntegrationTest {

    private static final ColumnDefinition<Integer> PARTITION_COLUMN = ofInt(KAFKA_PARTITION_COLUMN_NAME_DEFAULT);
    private static final ColumnDefinition<Long> OFFSET_COLUMN = ofLong(OFFSET_COLUMN_NAME_DEFAULT);
    private static final ColumnDefinition<Instant> TIMESTAMP_COLUMN = ofTime(TIMESTAMP_COLUMN_NAME_DEFAULT);

    private static final int TIMEOUT_SECONDS = 30;

    private EngineCleanup framework;
    private ControlledUpdateGraph updateGraph;

    @BeforeEach
    void setUp() throws Exception {
        framework = new EngineCleanup();
        framework.setUp();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
    }

    @AfterEach
    void tearDown() throws Exception {
        framework.tearDown();
    }

    @ParameterizedTest(name = "simpleKeySimpleValue {0}")
    @EnumSource
    @Timeout(TIMEOUT_SECONDS)
    void simpleKeySimpleValue(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
        Assumptions.assumeTrue(kafkaService.isEnabled());
        kafkaService.init();
        final String topic = sanitizedTopicName(testInfo);
        final String keyName = "Key";
        final String valueName = "Value";
        final TableDefinition td;
        final Table e1;
        final Table e2;
        final Table e3;
        {
            td = TableDefinition.of(
                    PARTITION_COLUMN,
                    OFFSET_COLUMN,
                    TIMESTAMP_COLUMN,
                    ofString(keyName),
                    ofString(valueName));
            e1 = TableTools.newTable(td);
            e2 = TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0, 0),
                    longCol(OFFSET_COLUMN.getName(), 0, 1),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(42L), Instant.ofEpochMilli(43L)),
                    stringCol(keyName, "key1", "key2"),
                    stringCol(valueName, "value1", "value2"));
            e3 = TableTools.merge(e2, TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0),
                    longCol(OFFSET_COLUMN.getName(), 2),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(44L)),
                    stringCol(keyName, "key3"),
                    stringCol(valueName, "value3")));
        }

        createTopic(kafkaService, topic);

        final KafkaTools.TableAndAdapter taa = KafkaTools.consumeToTableAndAdapter(
                kafkaService.properties(),
                topic,
                ALL_PARTITIONS,
                ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.simpleSpec(keyName, String.class),
                KafkaTools.Consume.simpleSpec(valueName, String.class),
                TableType.append());
        try (final KafkaProducer<String, String> producer =
                kafkaService.producer(new StringSerializer(), new StringSerializer())) {
            awaitEquals(e1, taa);

            producer.send(new ProducerRecord<>(topic, null, 42L, "key1", "value1"));
            producer.send(new ProducerRecord<>(topic, null, 43L, "key2", "value2"));
            producer.flush();

            awaitEquals(e2, taa);

            producer.send(new ProducerRecord<>(topic, null, 44L, "key3", "value3"));
            producer.flush();

            awaitEquals(e3, taa);
        }
    }

    @ParameterizedTest(name = "jsonKeySimpleValue {0}")
    @EnumSource
    @Timeout(TIMEOUT_SECONDS)
    void jsonKeySimpleValue(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
        Assumptions.assumeTrue(kafkaService.isEnabled());
        kafkaService.init();
        final String topic = sanitizedTopicName(testInfo);
        final String keyName = "Foo";
        final String valueName = "Value";
        final TableDefinition td;
        final Table e1;
        final Table e2;
        final Table e3;
        {
            td = TableDefinition.of(
                    PARTITION_COLUMN,
                    OFFSET_COLUMN,
                    TIMESTAMP_COLUMN,
                    ofString(keyName),
                    ofString(valueName));
            e1 = TableTools.newTable(td);
            e2 = TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0, 0),
                    longCol(OFFSET_COLUMN.getName(), 0, 1),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(42L), Instant.ofEpochMilli(43L)),
                    stringCol(keyName, "key1", "key2"),
                    stringCol(valueName, "value1", "value2"));
            e3 = TableTools.merge(e2, TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0),
                    longCol(OFFSET_COLUMN.getName(), 2),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(44L)),
                    stringCol(keyName, "key3"),
                    stringCol(valueName, "value3")));
        }

        createTopic(kafkaService, topic);

        final KafkaTools.TableAndAdapter taa = KafkaTools.consumeToTableAndAdapter(
                kafkaService.properties(),
                topic,
                ALL_PARTITIONS,
                ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.objectProcessorSpec(
                        JacksonProvider.of(ObjectValue.strict(Map.of("Foo", StringValue.strict())))),
                KafkaTools.Consume.simpleSpec(valueName, String.class),
                TableType.append());
        try (final KafkaProducer<String, String> producer =
                kafkaService.producer(new StringSerializer(), new StringSerializer())) {
            awaitEquals(e1, taa);

            producer.send(new ProducerRecord<>(topic, null, 42L, "{ \"Foo\": \"key1\" }", "value1"));
            producer.send(new ProducerRecord<>(topic, null, 43L, "{ \"Foo\": \"key2\" }", "value2"));
            producer.flush();

            awaitEquals(e2, taa);

            producer.send(new ProducerRecord<>(topic, null, 44L, "{ \"Foo\": \"key3\" }", "value3"));
            producer.flush();

            awaitEquals(e3, taa);
        }
    }

    @ParameterizedTest(name = "jsonKeyJsonValue {0}")
    @EnumSource
    @Timeout(TIMEOUT_SECONDS)
    void jsonKeyJsonValue(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
        Assumptions.assumeTrue(kafkaService.isEnabled());
        kafkaService.init();
        final String topic = sanitizedTopicName(testInfo);
        final String fooName = "Foo";
        final String barName = "Bar";
        final String zipName = "Zip";
        final String zapName = "Zap";

        final TableDefinition td;
        final Table e1;
        final Table e2;
        final Table e3;
        {
            td = TableDefinition.of(
                    PARTITION_COLUMN,
                    OFFSET_COLUMN,
                    TIMESTAMP_COLUMN,
                    ofString(fooName),
                    ofString(barName),
                    ofString(zipName),
                    ofString(zapName));
            e1 = TableTools.newTable(td);
            e2 = TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0, 0),
                    longCol(OFFSET_COLUMN.getName(), 0, 1),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(42L), Instant.ofEpochMilli(43L)),
                    stringCol(fooName, "foo1", "foo2"),
                    stringCol(barName, "bar1", "bar2"),
                    stringCol(zipName, "zip1", "zip2"),
                    stringCol(zapName, "zap1", "zap2"));
            e3 = TableTools.merge(e2, TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0),
                    longCol(OFFSET_COLUMN.getName(), 2),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(44L)),
                    stringCol(fooName, "foo3"),
                    stringCol(barName, "bar3"),
                    stringCol(zipName, "zip3"),
                    stringCol(zapName, "zap3")));
        }

        createTopic(kafkaService, topic);

        final KafkaTools.TableAndAdapter taa = KafkaTools.consumeToTableAndAdapter(
                kafkaService.properties(),
                topic,
                ALL_PARTITIONS,
                ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.objectProcessorSpec(
                        JacksonProvider.of(ObjectValue.builder()
                                .putFields("Foo", StringValue.strict())
                                .putFields("Bar", StringValue.strict())
                                .build())),
                KafkaTools.Consume.objectProcessorSpec(
                        JacksonProvider.of(ObjectValue.builder()
                                .putFields("Zip", StringValue.strict())
                                .putFields("Zap", StringValue.strict())
                                .build())),
                TableType.append());
        try (final KafkaProducer<String, String> producer =
                kafkaService.producer(new StringSerializer(), new StringSerializer())) {
            awaitEquals(e1, taa);

            producer.send(new ProducerRecord<>(topic, null, 42L, "{ \"Foo\": \"foo1\", \"Bar\": \"bar1\" }",
                    "{ \"Zip\": \"zip1\", \"Zap\": \"zap1\" }"));
            producer.send(new ProducerRecord<>(topic, null, 43L, "{ \"Foo\": \"foo2\", \"Bar\": \"bar2\" }",
                    "{ \"Zip\": \"zip2\", \"Zap\": \"zap2\" }"));
            producer.flush();

            awaitEquals(e2, taa);

            producer.send(new ProducerRecord<>(topic, null, 44L, "{ \"Foo\": \"foo3\", \"Bar\": \"bar3\" }",
                    "{ \"Zip\": \"zip3\", \"Zap\": \"zap3\" }"));
            producer.flush();

            awaitEquals(e3, taa);
        }
    }

    @ParameterizedTest(name = "jsonArrayTest_DH22657 {0}")
    @EnumSource
    @Timeout(TIMEOUT_SECONDS)
    void jsonArrayTest_DH22657(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
        Assumptions.assumeTrue(kafkaService.isEnabled());
        kafkaService.init();
        final String topic = sanitizedTopicName(testInfo);
        final String byteArrayName = "ByteArray";
        final String charArrayName = "CharArray";
        final String shortArrayName = "ShortArray";
        final String intArrayName = "IntArray";
        final String longArrayName = "LongArray";
        final String floatArrayName = "FloatArray";
        final String doubleArrayName = "DoubleArray";
        final String stringArrayName = "StringArray";

        final TableDefinition td;
        final Table e1;
        final Table e2;
        {
            td = TableDefinition.of(
                    PARTITION_COLUMN,
                    OFFSET_COLUMN,
                    TIMESTAMP_COLUMN,
                    ColumnDefinition.of(byteArrayName, Type.byteType().arrayType()),
                    ColumnDefinition.of(charArrayName, Type.charType().arrayType()),
                    ColumnDefinition.of(shortArrayName, Type.shortType().arrayType()),
                    ColumnDefinition.of(intArrayName, Type.intType().arrayType()),
                    ColumnDefinition.of(longArrayName, Type.longType().arrayType()),
                    ColumnDefinition.of(floatArrayName, Type.floatType().arrayType()),
                    ColumnDefinition.of(doubleArrayName, Type.doubleType().arrayType()),
                    ColumnDefinition.of(stringArrayName, Type.stringType().arrayType()));
            e1 = TableTools.newTable(td);
            e2 = TableTools.newTable(td,
                    intCol(PARTITION_COLUMN.getName(), 0, 0),
                    longCol(OFFSET_COLUMN.getName(), 0, 1),
                    instantCol(TIMESTAMP_COLUMN.getName(), Instant.ofEpochMilli(42L), Instant.ofEpochMilli(43L)),
                    new ColumnHolder<>(byteArrayName, byte[].class, byte.class, false,
                            new byte[] {1, 2, 3},
                            new byte[] {3, 2, 1}),
                    new ColumnHolder<>(charArrayName, char[].class, char.class, false,
                            new char[] {'a', 'b', 'c'},
                            new char[] {'d', 'e', 'f'}),
                    new ColumnHolder<>(shortArrayName, short[].class, short.class, false,
                            new short[] {1, 2, 3},
                            new short[] {3, 2, 1}),
                    new ColumnHolder<>(intArrayName, int[].class, int.class, false,
                            new int[] {1, 2, 3},
                            new int[] {3, 2, 1}),
                    new ColumnHolder<>(longArrayName, long[].class, long.class, false,
                            new long[] {1, 2, 3},
                            new long[] {3, 2, 1}),
                    new ColumnHolder<>(floatArrayName, float[].class, float.class, false,
                            new float[] {1, 2, 3},
                            new float[] {3, 2, 1}),
                    new ColumnHolder<>(doubleArrayName, double[].class, double.class, false,
                            new double[] {1, 2, 3},
                            new double[] {3, 2, 1}),
                    new ColumnHolder<>(stringArrayName, String[].class, String.class, false,
                            new String[] {"foo", "bar", "baz"},
                            new String[] {"baz", "bar", "foo"}));
        }

        createTopic(kafkaService, topic);

        final KafkaTools.TableAndAdapter taa = KafkaTools.consumeToTableAndAdapter(
                kafkaService.properties(),
                topic,
                ALL_PARTITIONS,
                ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.IGNORE,
                KafkaTools.Consume.objectProcessorSpec(
                        JacksonProvider.of(ObjectValue.builder()
                                .putFields(byteArrayName, ArrayValue.strict(ByteValue.standard()))
                                .putFields(charArrayName, ArrayValue.strict(CharValue.standard()))
                                .putFields(shortArrayName, ArrayValue.strict(ShortValue.standard()))
                                .putFields(intArrayName, ArrayValue.strict(IntValue.standard()))
                                .putFields(longArrayName, ArrayValue.strict(LongValue.standard()))
                                .putFields(floatArrayName, ArrayValue.strict(FloatValue.standard()))
                                .putFields(doubleArrayName, ArrayValue.strict(DoubleValue.standard()))
                                .putFields(stringArrayName, ArrayValue.strict(StringValue.standard()))
                                .build())),
                TableType.append());

        try (final KafkaProducer<Void, String> producer =
                kafkaService.producer(new VoidSerializer(), new StringSerializer())) {
            awaitEquals(e1, taa);

            producer.send(new ProducerRecord<>(topic, null, 42L, null,
                    "{ \"ByteArray\": [1, 2, 3], \"CharArray\": [\"a\", \"b\", \"c\"], \"ShortArray\": [1, 2, 3], \"IntArray\": [1, 2, 3], \"LongArray\": [1, 2, 3], \"FloatArray\": [1, 2, 3], \"DoubleArray\": [1, 2, 3], \"StringArray\": [\"foo\", \"bar\", \"baz\"] }"));
            producer.send(new ProducerRecord<>(topic, null, 43L, null,
                    "{ \"ByteArray\": [3, 2, 1], \"CharArray\": [\"d\", \"e\", \"f\"], \"ShortArray\": [3, 2, 1], \"IntArray\": [3, 2, 1], \"LongArray\": [3, 2, 1], \"FloatArray\": [3, 2, 1], \"DoubleArray\": [3, 2, 1], \"StringArray\": [\"baz\", \"bar\", \"foo\"] }"));
            producer.flush();

            awaitEquals(e2, taa);
        }
    }

    private static String sanitizedTopicName(TestInfo testInfo) {
        return testInfo.getDisplayName().replaceAll("[^a-zA-Z0-9._-]", "-");
    }

    private static void createTopic(final KafkaService kafkaService, final String topic)
            throws InterruptedException, ExecutionException {
        try (final AdminClient admin = kafkaService.admin()) {
            admin.createTopics(List.of(new NewTopic(topic, Optional.of(1), Optional.empty()))).all().get();
        }
    }

    private void awaitEquals(
            final Table expected,
            final KafkaTools.TableAndAdapter taa) throws InterruptedException {
        assert !expected.isRefreshing();
        updateGraph.runWithinUnitTestCycle(taa.adapter()::run);
        while (taa.table().size() != expected.size()) {
            // Note: we don't have a good mechanism to control, or be notified, when the Kafka consumer thread in
            // KafkaTools.consumeToTable consumes new data. As such, we'll continuously poll to see if our update source
            // has produced new data.
            Thread.sleep(50);
            updateGraph.runWithinUnitTestCycle(taa.adapter()::run);
        }
        TstUtils.assertTableEquals(expected, taa.table());
    }
}
