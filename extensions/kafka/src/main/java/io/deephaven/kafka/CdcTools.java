package io.deephaven.kafka;

import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.IntPredicate;

/**
 * Utility class with methods to support consuming from a Change Data Capture (CDC) Kafka stream (as, eg, produced by
 * Debezium) to a Deephaven table.
 *
 */
public class CdcTools {
    /**
     * The default schema name for the Key field of a given CDC topic is the topic name with this suffix added.
     */
    public static final String KEY_AVRO_SCHEMA_SUFFIX = "-key";
    /**
     * The default schema name for the Value field of a given CDC topic is the topic name with this suffix added.
     */
    public static final String VALUE_AVRO_SCHEMA_SUFFIX = "-value";
    /**
     * The default topic name for a CDC topic corresponds to the strings for server name, database name, and table name,
     * concatenated using this separator.
     */
    public static final String CDC_TOPIC_NAME_SEPARATOR = ".";
    /**
     * The Value Kafka field in a CDC topic contains a sub field with a record (as nested fields) with values for all
     * the columns for an updated (or added) row. This constant should match the path from the root, to this record. For
     * instance, if the parent field is called "after", and the nested field separator is ".", this constant should be
     * "after."
     */
    public static final String CDC_AFTER_COLUMN_PREFIX = "after" + KafkaTools.NESTED_FIELD_NAME_SEPARATOR;
    /**
     * The name of the sub-field in the Value field that indicates the type of operation that triggered the CDC event
     * (eg, insert, delete).
     */
    public static final String CDC_OP_COLUMN_NAME = "op";
    /**
     * The value for the sub-field that indicates the type of operation for when the operation type is delete.
     */
    public static final String CDC_DELETE_OP_VALUE = "d";

    /**
     * Users specify CDC streams via objects satisfying this interface; the objects are created with static factory
     * methods, the classes implementing this interface are opaque from a user perspective.
     */
    private interface CdcSpec {
        /**
         * @return CDC stream kafka topic
         */
        String topic();

        /**
         * return Avro Schema name in Schema Service for the Key Kafka field.
         */
        String keySchemaName();

        /**
         * @return Version to use from Schema Service for the Avro Schema for the Key kafka field.
         */
        String keySchemaVersion();

        /**
         * return Avro Schema name in Schema Service for the Value Kafka field.
         */
        String valueSchemaName();

        /**
         * @return Version to use from Schema Service for the Avro Schema for the Value kafka field.
         */
        String valueSchemaVersion();
    }

    /**
     * Specify a CDC stream by extension.
     */
    private static class CdcSpecTopicSchemas implements CdcSpec {
        public final String topic;
        public final String keySchemaName;
        public final String keySchemaVersion;
        public final String valueSchemaName;
        public final String valueSchemaVersion;

        private CdcSpecTopicSchemas(
                final String topic,
                final String keySchemaName,
                final String keySchemaVersion,
                final String valueSchemaName,
                final String valueSchemaVersion) {
            this.topic = topic;
            this.keySchemaName = keySchemaName;
            this.keySchemaVersion = keySchemaVersion;
            this.valueSchemaName = valueSchemaName;
            this.valueSchemaVersion = valueSchemaVersion;
        }

        @Override
        public String topic() {
            return topic;
        }

        @Override
        public String keySchemaName() {
            return keySchemaName;
        }

        @Override
        public String keySchemaVersion() {
            return keySchemaVersion;
        }

        @Override
        public String valueSchemaName() {
            return valueSchemaName;
        }

        @Override
        public String valueSchemaVersion() {
            return valueSchemaVersion;
        }
    }

    /**
     * Specify a CDC stream by server name, database name, and table name.
     */
    private static class CdcSpecServerDbTable implements CdcSpec {
        public final String serverName;
        public final String dbName;
        public final String tableName;

        private CdcSpecServerDbTable(
                final String serverName,
                final String dbName,
                final String tableName) {
            this.serverName = serverName;
            this.dbName = dbName;
            this.tableName = tableName;
        }

        @Override
        public String topic() {
            return cdcTopicName(serverName, dbName, tableName);
        }

        @Override
        public String keySchemaName() {
            return cdcKeyAvroSchemaName(serverName, dbName, tableName);
        }

        @Override
        public String keySchemaVersion() {
            return KafkaTools.AVRO_LATEST_VERSION;
        }

        @Override
        public String valueSchemaName() {
            return cdcValueAvroSchemaName(serverName, dbName, tableName);
        }

        @Override
        public String valueSchemaVersion() {
            return KafkaTools.AVRO_LATEST_VERSION;
        }
    }

    /**
     * Create a {@code CdcSpec} opaque object (necessary for one argument in a call to consume*ToTable) via explicitly
     * specifying topic name, and key and value schema names.
     *
     * @param topic The Kafka topic for the CDC events associated to the desired table data.
     * @param keySchemaName The schema name for the Key Kafka field in the CDC events for the topic. This schema should
     *        include definitions for the columns forming the PRIMARY KEY of the underlying table.
     * @param valueSchemaName The schema name for the Value Kafka field in the CDC events for the topic. This schema
     *        should include definitions for all the columns of the underlying table.
     * @return A CdcSpec object corresponding to the inputs; schema versions are implied to be latest.
     */
    @ScriptApi
    public static CdcSpec cdcLongSpec(
            final String topic,
            final String keySchemaName,
            final String valueSchemaName) {
        return new CdcSpecTopicSchemas(topic, keySchemaName, KafkaTools.AVRO_LATEST_VERSION, valueSchemaName,
                KafkaTools.AVRO_LATEST_VERSION);
    }

    /**
     * Create a {@code CdcSpec} opaque object (necessary for one argument in a call to consume*ToTable) via explicitly
     * specifying all configuration options.
     *
     * @param topic The Kafka topic for the CDC events associated to the desired table data.
     * @param keySchemaName The schema name for the Key Kafka field in the CDC events for the topic. This schema should
     *        include definitions for the columns forming the PRIMARY KEY of the underlying table.
     * @parar keySchemaVersion The version for the Key schema to look up in schema server.
     * @param valueSchemaName The schema name for the Value Kafka field in the CDC events for the topic. This schema
     *        should include definitions for all the columns of the underlying table.
     * @param valueSchemaVersion The version for the Value schema to look up in schema server.
     * @return A CdcSpec object corresponding to the inputs.
     */
    @ScriptApi
    public static CdcSpec cdcLongSpec(
            final String topic,
            final String keySchemaName,
            final String keySchemaVersion,
            final String valueSchemaName,
            final String valueSchemaVersion) {
        return new CdcSpecTopicSchemas(topic, keySchemaName, keySchemaVersion, valueSchemaName, valueSchemaVersion);
    }

    /**
     * Create a {@code CdcSpec} opaque object (necessary for one argument in a call to consume*ToTable) in the debezium
     * style, specifying server name, database name and table name. The topic name, and key and value schema names are
     * implied by convention:
     * <ul>
     * <li>Topic is the concatenation of the arguments using "." as separator.</li>
     * <li>Key schema name is topic with a "-key" suffix added.</li>
     * <li>Value schema name is topic with a "-value" suffix added.</li>
     * </ul>
     *
     * @param serverName The server name
     * @param dbName The database name
     * @param tableName The table name
     *
     * @return A CdcSpec object corresponding to the inputs.
     */
    @ScriptApi
    public static CdcSpec cdcShortSpec(
            final String serverName,
            final String dbName,
            final String tableName) {
        return new CdcSpecServerDbTable(serverName, dbName, tableName);
    }

    /**
     * Consume from a CDC Kafka Event Stream to a DHC ticking table, recreating the underlying database table.
     *
     * @param kafkaProperties Properties to configure the associated kafka consumer and also the resulting table. Passed
     *        to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any KafkaConsumer specific
     *        desired configuration here. Note this should include the relevant property for a schema server URL where
     *        the key and/or value Avro necessary schemas are stored.
     * @param cdcSpec A CdcSpec opaque object specifying the CDC Stream. Can be obtained from calling the
     *        {@code cdcSpec} static factory method.
     * @param partitionFilter A function specifying the desired initial offset for each partition consumed The
     *        convenience constant {@code KafkaTools.ALL_PARTITIONS} is defined to facilitate requesting all partitions.
     * @return A Deephaven live table for underlying database table tracked by the CDC Stream
     */
    @ScriptApi
    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final CdcSpec cdcSpec,
            @NotNull final IntPredicate partitionFilter) {
        return consumeToTable(
                kafkaProperties,
                cdcSpec,
                partitionFilter,
                false,
                null);
    }

    /**
     * Consume from a CDC Kafka Event Stream to a DHC ticking table, recreating the underlying database table.
     *
     * @param kafkaProperties Properties to configure the associated kafka consumer and also the resulting table. Passed
     *        to the org.apache.kafka.clients.consumer.KafkaConsumer constructor; pass any KafkaConsumer specific
     *        desired configuration here. Note this should include the relevant property for a schema server URL where
     *        the key and/or value Avro necessary schemas are stored.
     * @param cdcSpec A CdcSpec opaque object specifying the CDC Stream. Can be obtained from calling the
     *        {@code cdcSpec} static factory method.
     * @param partitionFilter A function specifying the desired initial offset for each partition consumed The
     *        convenience constant {@code KafkaTools.ALL_PARTITIONS} is defined to facilitate requesting all partitions.
     * @param asStreamTable If true, return a stream table of row changes with an added 'op' column including the CDC
     *        operation affecting the row.
     * @param dropColumns Collection of column names that will be dropped from the resulting table; null for none. Note
     *        that only columns not included in the primary key can be dropped at this stage; you can chain a drop
     *        column operation after this call if you need to drop primary key columns.
     * @return A Deephaven live table for underlying database table tracked by the CDC Stream
     */
    @ScriptApi
    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final CdcSpec cdcSpec,
            @NotNull final IntPredicate partitionFilter,
            final boolean asStreamTable,
            Collection<String> dropColumns) {
        final Schema valueSchema = KafkaTools.getAvroSchema(
                kafkaProperties, cdcSpec.valueSchemaName(), cdcSpec.valueSchemaVersion());
        final Schema keySchema = KafkaTools.getAvroSchema(
                kafkaProperties, cdcSpec.keySchemaName(), cdcSpec.keySchemaVersion());
        final Table streamingIn = KafkaTools.consumeToTable(
                kafkaProperties,
                cdcSpec.topic(),
                partitionFilter,
                KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.avroSpec(keySchema),
                KafkaTools.Consume.avroSpec(valueSchema),
                KafkaTools.TableType.Stream);
        final List<String> dbTableColumnNames = dbTableColumnNames(streamingIn);
        List<String> allDroppedColumns = null;
        if (dropColumns != null && dropColumns.size() > 0) {
            allDroppedColumns = new ArrayList<>(dropColumns);
        }
        if (!asStreamTable) {
            if (allDroppedColumns == null) {
                allDroppedColumns = new ArrayList<>(1);
            }
            allDroppedColumns.add(CDC_OP_COLUMN_NAME);
        }
        final List<String> dbTableKeyColumnNames = fieldNames(keySchema);
        final Table narrowerStreamingTable = streamingIn
                .view(narrowerStreamingTableViewExpressions(dbTableKeyColumnNames, dbTableColumnNames));
        if (asStreamTable) {
            if (allDroppedColumns != null) {
                return narrowerStreamingTable.dropColumns(allDroppedColumns);
            }
            return narrowerStreamingTable;
        }
        final List<String> lastByColumnNames;
        // @formatter:off
        final Table cdc = narrowerStreamingTable
                .lastBy(dbTableKeyColumnNames)
                .where(CDC_OP_COLUMN_NAME + " != `" + CDC_DELETE_OP_VALUE + "`")
                .dropColumns(allDroppedColumns);
        // @formatter:on
        return cdc;
    }

    @ScriptApi
    public static Table consumeRawToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final CdcSpec cdcSpec,
            @NotNull final IntPredicate partitionFilter,
            @NotNull final KafkaTools.TableType tableType) {
        return KafkaTools.consumeToTable(
                kafkaProperties,
                cdcSpec.topic(),
                partitionFilter,
                KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
                KafkaTools.Consume.avroSpec(cdcSpec.keySchemaName(), cdcSpec.keySchemaVersion()),
                KafkaTools.Consume.avroSpec(cdcSpec.valueSchemaName(), cdcSpec.valueSchemaVersion()),
                tableType);
    }

    private static String[] narrowerStreamingTableViewExpressions(
            final List<String> dbTableKeyColumnNames,
            final List<String> dbTableColumnNames) {
        final String[] viewExpressions = new String[dbTableColumnNames.size() + 1];
        int i = 0;
        final Set<String> keyColumnsSet = new HashSet(dbTableKeyColumnNames);
        for (final String columnName : dbTableColumnNames) {
            if (dbTableKeyColumnNames.contains(columnName)) {
                viewExpressions[i++] = columnName;
            } else {
                viewExpressions[i++] = columnName + "=" + CDC_AFTER_COLUMN_PREFIX + columnName;
            }
        }
        viewExpressions[i++] = CDC_OP_COLUMN_NAME;
        return viewExpressions;
    }

    private static List<String> dbTableColumnNames(final Table streamingIn) {
        final List<String> columnNames = new ArrayList<>();
        final int nameOffset = CDC_AFTER_COLUMN_PREFIX.length();
        for (final DataColumn<?> col : streamingIn.getColumns()) {
            final String name = col.getName();
            if (name.startsWith(CDC_AFTER_COLUMN_PREFIX)) {
                columnNames.add(name.substring(nameOffset));
            }
        }
        return columnNames;
    }

    private static List<String> fieldNames(final Schema schema) {
        final List<String> fieldNames = new ArrayList<>();
        for (final Schema.Field field : schema.getFields()) {
            fieldNames.add(field.name());
        }
        return fieldNames;
    }

    /**
     * Build the default CDC topic name given server name, database name and table name
     *
     * @param serverName The server name
     * @param dbName The database name
     * @param tableName The table name
     * @return The default CDC topic name given inputs.
     */
    @ScriptApi
    public static String cdcTopicName(final String serverName, final String dbName, final String tableName) {
        return serverName + CDC_TOPIC_NAME_SEPARATOR +
                dbName + CDC_TOPIC_NAME_SEPARATOR +
                tableName;
    }

    /**
     * Build the default Key schema name for a CDC Stream, given server name, database name and table name
     *
     * @param serverName The server name
     * @param dbName The database name
     * @param tableName The table name
     * @return The default Key schema name for a CDC Stream given inputs.
     */
    @ScriptApi
    public static String cdcKeyAvroSchemaName(final String serverName, final String dbName, final String tableName) {
        return cdcTopicName(serverName, dbName, tableName) + KEY_AVRO_SCHEMA_SUFFIX;
    }

    /**
     * Build the default Value schema name for a CDC Stream, given server name, database name and table name
     *
     * @param serverName The server name
     * @param dbName The database name
     * @param tableName The table name
     * @return The default Value schema name for a CDC Stream given inputs.
     */
    @ScriptApi
    public static String cdcValueAvroSchemaName(final String serverName, final String dbName, final String tableName) {
        return cdcTopicName(serverName, dbName, tableName) + VALUE_AVRO_SCHEMA_SUFFIX;
    }
}
