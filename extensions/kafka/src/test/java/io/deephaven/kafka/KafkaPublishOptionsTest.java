/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.util.TableTools;
import io.deephaven.kafka.KafkaTools.Produce;
import org.junit.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class KafkaPublishOptionsTest {

    private static final TableDefinition TD = TableDefinition.of(
            ColumnDefinition.ofString("MyTopic"),
            ColumnDefinition.ofInt("MyPartition"),
            ColumnDefinition.ofTime("MyTimestamp"),
            ColumnDefinition.ofString("MyValue"));

    @Test
    public void ok() {
        KafkaPublishOptions.builder()
                .table(TableTools.newTable(TD))
                .topic("HotTopic")
                .config(new Properties())
                .valueSpec(Produce.simpleSpec("MyValue"))
                .build();
    }

    @Test
    public void okPartition() {
        KafkaPublishOptions.builder()
                .table(TableTools.newTable(TD))
                .topic("HotTopic")
                .partition(123)
                .config(new Properties())
                .valueSpec(Produce.simpleSpec("MyValue"))
                .build();
    }


    @Test
    public void checkNotBothIgnore() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("keySpec and valueSpec can't both be ignore specs");
        }
    }

    @Test
    public void checkPublishInitial() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .publishInitial(false)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("publishInitial==false && table.isRefreshing() == false");
        }
    }

    @Test
    public void checkLastBy() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .lastBy(true)
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Must set a non-ignore keySpec when lastBy() == true");
        }
        KafkaPublishOptions.builder()
                .table(TableTools.newTable(TD))
                .topic("HotTopic")
                .config(new Properties())
                .keySpec(Produce.simpleSpec("MyValue"))
                .lastBy(true)
                .build();
    }

    @Test
    public void checkTopic() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .build();
            failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
        } catch (IllegalArgumentException e) {
            assertThat(e).hasMessage("Must set topic or topicColumn (or both)");
        }
    }

    @Test
    public void checkTopicColumn() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .topicColumn(ColumnName.of("DoesNotExist"))
                    .build();
            failBecauseExceptionWasNotThrown(NoSuchColumnException.class);
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [DoesNotExist]");
        }
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .topicColumn(ColumnName.of("MyPartition"))
                    .build();
            failBecauseExceptionWasNotThrown(ClassCastException.class);
        } catch (ClassCastException e) {
            assertThat(e).hasMessage("Cannot convert [MyPartition] of type int to type java.lang.CharSequence");
        }
        KafkaPublishOptions.builder()
                .table(TableTools.newTable(TD))
                .config(new Properties())
                .valueSpec(Produce.simpleSpec("MyValue"))
                .topicColumn(ColumnName.of("MyTopic"))
                .build();
    }

    @Test
    public void checkPartitionColumn() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .partitionColumn(ColumnName.of("DoesNotExist"))
                    .build();
            failBecauseExceptionWasNotThrown(NoSuchColumnException.class);
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [DoesNotExist]");
        }
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .partitionColumn(ColumnName.of("MyTopic"))
                    .build();
            failBecauseExceptionWasNotThrown(ClassCastException.class);
        } catch (ClassCastException e) {
            assertThat(e).hasMessage("Cannot convert [MyTopic] of type java.lang.String to type int");
        }
        KafkaPublishOptions.builder()
                .table(TableTools.newTable(TD))
                .topic("HotTopic")
                .config(new Properties())
                .valueSpec(Produce.simpleSpec("MyValue"))
                .partitionColumn(ColumnName.of("MyPartition"))
                .build();
    }

    @Test
    public void checkTimestampColumn() {
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .timestampColumn(ColumnName.of("DoesNotExist"))
                    .build();
            failBecauseExceptionWasNotThrown(NoSuchColumnException.class);
        } catch (NoSuchColumnException e) {
            assertThat(e).hasMessageContaining("Unknown column names [DoesNotExist]");
        }
        try {
            KafkaPublishOptions.builder()
                    .table(TableTools.newTable(TD))
                    .topic("HotTopic")
                    .config(new Properties())
                    .valueSpec(Produce.simpleSpec("MyValue"))
                    .timestampColumn(ColumnName.of("MyTopic"))
                    .build();
            failBecauseExceptionWasNotThrown(ClassCastException.class);
        } catch (ClassCastException e) {
            assertThat(e).hasMessage("Cannot convert [MyTopic] of type java.lang.String to type java.time.Instant");
        }
        KafkaPublishOptions.builder()
                .table(TableTools.newTable(TD))
                .topic("HotTopic")
                .config(new Properties())
                .valueSpec(Produce.simpleSpec("MyValue"))
                .timestampColumn(ColumnName.of("MyTimestamp"))
                .build();
    }
}
