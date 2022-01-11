package io.deephaven.kafka;

import io.deephaven.engine.table.Table;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.function.IntPredicate;

public class CdcTools {
    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter) {
        return consumeToTable(kafkaProperties, topic, partitionFilter, false);
    }

    public static Table consumeToTable(
            @NotNull final Properties kafkaProperties,
            @NotNull final String topic,
            @NotNull final IntPredicate partitionFilter,
            final boolean ignoreKey) {
        return KafkaTools.consumeToTable(
                kafkaProperties,
                topic,
                partitionFilter,
                KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
                ignoreKey
                        ? KafkaTools.Consume.IGNORE
                        : KafkaTools.Consume.avroSpec(topic + "-key"),
                KafkaTools.Consume.avroSpec(topic + "-value"),
                KafkaTools.TableType.Append);
    }
}
