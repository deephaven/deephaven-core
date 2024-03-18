//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.Table;
import io.deephaven.kafka.KafkaTools.Produce;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

/**
 * The options to produce a Kafka stream from a Deephaven table.
 *
 * @see KafkaTools#produceFromTable(KafkaPublishOptions)
 */
@Immutable
@BuildableStyle
public abstract class KafkaPublishOptions {

    public static Builder builder() {
        return ImmutableKafkaPublishOptions.builder();
    }

    /**
     * The table used as a source of data to be sent to Kafka.
     *
     * @return the table
     */
    public abstract Table table();

    /**
     * The default Kafka topic to publish to. When {@code null}, {@link #topicColumn()} must be set.
     *
     * @return the default Kafka topic
     * @see #topicColumn()
     */
    @Nullable
    public abstract String topic();

    /**
     * The default Kafka partition to publish to.
     *
     * @return the default Kafka partition
     * @see #partitionColumn()
     */
    public abstract OptionalInt partition();

    /**
     * The Kafka configuration properties.
     *
     * @return the Kafka configuration
     */
    public abstract Properties config();

    /**
     * The conversion specification for Kafka record keys from table column data. By default, is
     * {@link Produce#ignoreSpec()}.
     *
     * @return the key spec
     */
    @Default
    public KeyOrValueSpec keySpec() {
        return Produce.ignoreSpec();
    }

    /**
     * The conversion specification for Kafka record values from table column data. By default, is
     * {@link Produce#ignoreSpec()}.
     *
     * @return the value spec
     */
    @Default
    public KeyOrValueSpec valueSpec() {
        return Produce.ignoreSpec();
    }

    /**
     * Whether to publish only the last record for each unique key. If {@code true}, the publishing logic will
     * internally perform a {@link Table#lastBy(String...) lastBy} aggregation on {@link #table()} grouped by the input
     * columns of {@link #keySpec()}. When {@code true}, the {@link #keySpec() keySpec} must not be
     * {@link Produce#ignoreSpec() ignoreSpec}. By default, is {@code false}.
     *
     * @return if the publishing should be done with a last-by table
     */
    @Default
    public boolean lastBy() {
        return false;
    }

    /**
     * If the initial data in {@link #table()} should be published. When {@code false}, {@link #table()} must be
     * {@link Table#isRefreshing() refreshing}. By default, is {@code true}.
     * 
     * @return if the initial table data should be published
     */
    @Default
    public boolean publishInitial() {
        return true;
    }

    /**
     * The topic column. When set, uses the the given {@link CharSequence}-compatible column from {@link #table()} as
     * the first source for setting the Kafka record topic. When not present, or if the column value is null,
     * {@link #topic()} will be used.
     *
     * @return the topic column name
     */
    public abstract Optional<ColumnName> topicColumn();

    /**
     * The partition column. When set, uses the the given {@code int} column from {@link #table()} as the first source
     * for setting the Kafka record partition. When not present, or if the column value is null, {@link #partition()}
     * will be used if present. If a valid partition number is specified, that partition will be used when sending the
     * record. Otherwise, Kafka will choose a partition using a hash of the key if the key is present, or will assign a
     * partition in a round-robin fashion if the key is not present.
     *
     * @return the partition column name
     */
    public abstract Optional<ColumnName> partitionColumn();

    /**
     * The timestamp column. When set, uses the the given {@link Instant} column from {@link #table()} as the first
     * source for setting the Kafka record timestamp. When not present, or if the column value is null, the producer
     * will stamp the record with its current time. The timestamp eventually used by Kafka depends on the timestamp type
     * configured for the topic. If the topic is configured to use CreateTime, the timestamp in the producer record will
     * be used by the broker. If the topic is configured to use LogAppendTime, the timestamp in the producer record will
     * be overwritten by the broker with the broker local time when it appends the message to its log.
     *
     * @return the timestamp column name
     */
    public abstract Optional<ColumnName> timestampColumn();

    @Check
    final void checkNotBothIgnore() {
        if (Produce.isIgnore(keySpec()) && Produce.isIgnore(valueSpec())) {
            throw new IllegalArgumentException("keySpec and valueSpec can't both be ignore specs");
        }
    }

    @Check
    final void checkPublishInitial() {
        if (!publishInitial() && !table().isRefreshing()) {
            throw new IllegalArgumentException("publishInitial==false && table.isRefreshing() == false");
        }
    }

    @Check
    final void checkLastBy() {
        if (lastBy() && Produce.isIgnore(keySpec())) {
            throw new IllegalArgumentException("Must set a non-ignore keySpec when lastBy() == true");
        }
    }

    @Check
    final void checkTopic() {
        if (topic() == null && topicColumn().isEmpty()) {
            throw new IllegalArgumentException("Must set topic or topicColumn (or both)");
        }
    }

    @Check
    final void checkTopicColumn() {
        if (topicColumn().isPresent()) {
            table().getDefinition().checkHasColumn(topicColumn().get().name(), CharSequence.class);
        }
    }

    @Check
    final void checkPartitionColumn() {
        if (partitionColumn().isPresent()) {
            table().getDefinition().checkHasColumn(partitionColumn().get().name(), int.class);
        }
    }

    @Check
    final void checkTimestampColumn() {
        if (timestampColumn().isPresent()) {
            table().getDefinition().checkHasColumn(timestampColumn().get().name(), Instant.class);
        }
    }

    public interface Builder {

        Builder table(Table table);

        Builder topic(String topic);

        Builder partition(int partition);

        Builder config(Properties config);

        Builder keySpec(KeyOrValueSpec keySpec);

        Builder valueSpec(KeyOrValueSpec valueSpec);

        Builder lastBy(boolean lastBy);

        Builder publishInitial(boolean publishInitial);

        Builder topicColumn(ColumnName columnName);

        Builder partitionColumn(ColumnName columnName);

        Builder timestampColumn(ColumnName columnName);

        KafkaPublishOptions build();
    }
}
