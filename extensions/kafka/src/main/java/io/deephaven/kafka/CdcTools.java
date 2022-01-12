package io.deephaven.kafka;

import io.deephaven.engine.table.DataColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.StreamTableTools;
import org.jetbrains.annotations.NotNull;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.IntPredicate;

public class CdcTools {
    public static final String KEY_AVRO_SCHEMA_SUFFIX = "-key";
    public static final String VALUE_AVRO_SCHEMA_SUFFIX = "-value";
    public static final String CDC_TOPIC_NAME_SEPARATOR = ".";
    public static final String CDC_AFTER_COLUMN_PREFIX = "after__";
    public static final String CDC_OP_COLUMN_NAME = "op";
    public static final String CDC_DELETE_OP_VALUE = "d";

    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String cdcServerName,
            @NotNull final String cdcDbName,
            @NotNull final String cdcTableName,
            @NotNull final IntPredicate partitionFilter) {
        return consumeToTable(
                kafkaProperties,
                cdcServerName,
                cdcDbName,
                cdcTableName,
                partitionFilter,
                false,
                false
        );
    }

    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String cdcServerName,
            @NotNull final String cdcDbName,
            @NotNull final String cdcTableName,
            @NotNull final IntPredicate partitionFilter,
            final boolean ignoreKey,
            final boolean appendOnlySource) {
        final String valueSchemaName = cdcValueAvroSchemaName(cdcServerName, cdcDbName, cdcTableName);
        final Schema valueSchema = KafkaTools.getAvroSchema(kafkaProperties, valueSchemaName);
        final Schema keySchema;
        if (ignoreKey) {
            keySchema = null;
        } else {
            final String keySchemaName = cdcKeyAvroSchemaName(cdcServerName, cdcDbName, cdcTableName);
            keySchema = KafkaTools.getAvroSchema(kafkaProperties, keySchemaName);
        }
        final String topic = cdcTopicName(cdcServerName, cdcDbName, cdcTableName);
        final Table streamingIn = KafkaTools.consumeToTable(
                kafkaProperties,
                topic,
                partitionFilter,
                KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
                ignoreKey
                        ? KafkaTools.Consume.IGNORE
                        : KafkaTools.Consume.avroSpec(keySchema),
                KafkaTools.Consume.avroSpec(valueSchema),
                KafkaTools.TableType.Stream);
        final List<String> dbTableColumnNames = dbTableColumnNames(streamingIn);
        final Table narrowerStreamingTable = streamingIn
                .view(narrowerStreamingTableViewExpressions(dbTableColumnNames, appendOnlySource));
        if (appendOnlySource) {
            return StreamTableTools.streamToAppendOnlyTable(narrowerStreamingTable);
        }
        final List<String> lastByColumnNames;
        if (ignoreKey) {
            lastByColumnNames = fieldNames(valueSchema);
        } else {
            lastByColumnNames = fieldNames(keySchema);
        }
        final Table cdc = narrowerStreamingTable
                .lastBy(lastByColumnNames)
                .where(CDC_OP_COLUMN_NAME + " != `" + CDC_DELETE_OP_VALUE + "`")
                .dropColumns(CDC_OP_COLUMN_NAME);
        return cdc;
    }

    private static String[] narrowerStreamingTableViewExpressions(
            final List<String> dbTableColumnNames,
            final boolean appendOnlySource) {
        final String[] viewExpressions = new String[dbTableColumnNames.size() + 1];
        int i = 0;
        for (final String columnName : dbTableColumnNames) {
            viewExpressions[i++] = columnName + "=" + CDC_AFTER_COLUMN_PREFIX + columnName;
        }
        if (!appendOnlySource) {
            viewExpressions[i++] = CDC_OP_COLUMN_NAME;
        }
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

    private static String cdcTopicName(final String serverName, final String dbName, final String tableName) {
        return serverName + CDC_TOPIC_NAME_SEPARATOR +
                dbName + CDC_TOPIC_NAME_SEPARATOR +
                tableName;
    }

    public static String cdcKeyAvroSchemaName(final String serverName, final String dbName, final String tableName) {
        return cdcTopicName(serverName, dbName, tableName) + KEY_AVRO_SCHEMA_SUFFIX;
    }

    public static String cdcValueAvroSchemaName(final String serverName, final String dbName, final String tableName) {
        return cdcTopicName(serverName, dbName, tableName) + VALUE_AVRO_SCHEMA_SUFFIX;
    }
}
