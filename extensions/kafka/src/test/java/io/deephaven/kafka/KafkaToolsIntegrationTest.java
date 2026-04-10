//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.json.ObjectValue;
import io.deephaven.json.StringValue;
import io.deephaven.json.jackson.JacksonProvider;
import io.deephaven.kafka.KafkaTools.TableType;
import io.deephaven.kafka.testcontainers.KafkaService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
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
    @Timeout(10)
    void simpleKeySimpleValue(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
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
    @Timeout(10)
    void jsonKeySimpleValue(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
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
    @Timeout(10)
    void jsonKeyJsonValue(final KafkaService kafkaService, final TestInfo testInfo) throws Exception {
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
