//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins the schemas of the two barrage performance tables and verifies that the publishers write the values they are
 * given verbatim.
 * <p>
 * Both tables report raw units -- nanoseconds and bytes as {@code long} -- matching {@code UpdatePerformanceLog} and
 * the integrator sinks. A change to a column name, a column type, or a scale factor is a break in a published data
 * contract, so it should fail here rather than surprise a downstream query.
 */
public class BarragePerformanceStreamPublisherTest {

    @Test
    public void subscriptionDefinitionIsRawLongUnits() {
        assertThat(BarrageSubscriptionPerformanceStreamPublisher.definition()).isEqualTo(TableDefinition.of(
                ColumnDefinition.ofString("TableId"),
                ColumnDefinition.ofString("TableKey"),
                ColumnDefinition.ofString("StatType"),
                ColumnDefinition.ofTime("Time"),
                ColumnDefinition.ofLong("Count"),
                ColumnDefinition.ofLong("Pct50"),
                ColumnDefinition.ofLong("Pct75"),
                ColumnDefinition.ofLong("Pct90"),
                ColumnDefinition.ofLong("Pct95"),
                ColumnDefinition.ofLong("Pct99"),
                ColumnDefinition.ofLong("Max")));
    }

    @Test
    public void snapshotDefinitionIsRawLongUnits() {
        assertThat(BarrageSnapshotPerformanceStreamPublisher.definition()).isEqualTo(TableDefinition.of(
                ColumnDefinition.ofString("TableId"),
                ColumnDefinition.ofString("TableKey"),
                ColumnDefinition.ofTime("RequestTime"),
                ColumnDefinition.ofLong("QueueNanos"),
                ColumnDefinition.ofLong("SnapshotNanos"),
                ColumnDefinition.ofLong("WriteNanos"),
                ColumnDefinition.ofLong("WriteBytes")));
    }

    @Test
    public void subscriptionPublisherWritesValuesVerbatim() {
        final BarrageSubscriptionPerformanceStreamPublisher publisher =
                new BarrageSubscriptionPerformanceStreamPublisher();
        final RecordingStreamConsumer consumer = new RecordingStreamConsumer();
        publisher.register(consumer);

        publisher.add("id", "key", BarrageSubscriptionPerformanceLogger.StatType.WRITE_NANOS,
                1234567890123L, 7L, 11L, 22L, 33L, 44L, 55L, 66L);
        publisher.flush();

        assertThat(consumer.stringAt(0, 0)).isEqualTo("id");
        assertThat(consumer.stringAt(1, 0)).isEqualTo("key");
        assertThat(consumer.stringAt(2, 0)).isEqualTo("WriteNanos");
        assertThat(consumer.longAt(3, 0)).isEqualTo(1234567890123L);
        assertThat(consumer.longAt(4, 0)).isEqualTo(7L);
        assertThat(consumer.longAt(5, 0)).isEqualTo(11L);
        assertThat(consumer.longAt(6, 0)).isEqualTo(22L);
        assertThat(consumer.longAt(7, 0)).isEqualTo(33L);
        assertThat(consumer.longAt(8, 0)).isEqualTo(44L);
        assertThat(consumer.longAt(9, 0)).isEqualTo(55L);
        assertThat(consumer.longAt(10, 0)).isEqualTo(66L);
    }

    @Test
    public void snapshotPublisherWritesValuesVerbatim() {
        final BarrageSnapshotPerformanceStreamPublisher publisher = new BarrageSnapshotPerformanceStreamPublisher();
        final RecordingStreamConsumer consumer = new RecordingStreamConsumer();
        publisher.register(consumer);

        publisher.add("id", "key", 1234567890123L, 100L, 200L, 300L, 4096L);
        publisher.flush();

        assertThat(consumer.stringAt(0, 0)).isEqualTo("id");
        assertThat(consumer.stringAt(1, 0)).isEqualTo("key");
        assertThat(consumer.longAt(2, 0)).isEqualTo(1234567890123L);
        assertThat(consumer.longAt(3, 0)).isEqualTo(100L);
        assertThat(consumer.longAt(4, 0)).isEqualTo(200L);
        assertThat(consumer.longAt(5, 0)).isEqualTo(300L);
        assertThat(consumer.longAt(6, 0)).isEqualTo(4096L);
    }

    @Test
    public void emptyFlushPublishesNothing() {
        final BarrageSubscriptionPerformanceStreamPublisher publisher =
                new BarrageSubscriptionPerformanceStreamPublisher();
        final RecordingStreamConsumer consumer = new RecordingStreamConsumer();
        publisher.register(consumer);

        publisher.flush();

        assertThat(consumer.batchCount()).isZero();
    }

    @Test
    public void shutdownDiscardsPendingRowsAndIgnoresFurtherTraffic() {
        final BarrageSubscriptionPerformanceStreamPublisher publisher =
                new BarrageSubscriptionPerformanceStreamPublisher();
        final RecordingStreamConsumer consumer = new RecordingStreamConsumer();
        publisher.register(consumer);

        publisher.add("id", "key", BarrageSubscriptionPerformanceLogger.StatType.WRITE_NANOS,
                1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L);
        publisher.shutdown();

        // Shutdown means the blink table is gone, so a pending row is released rather than delivered.
        assertThat(consumer.batchCount()).isZero();

        // Once shut down, the publisher ignores further traffic instead of failing; the impls keep calling add()
        // after the blink table has been destroyed, and shutdown() itself may be invoked more than once.
        publisher.add("id", "key", BarrageSubscriptionPerformanceLogger.StatType.WRITE_NANOS,
                2L, 2L, 2L, 2L, 2L, 2L, 2L, 2L);
        publisher.flush();
        publisher.shutdown();
        assertThat(consumer.batchCount()).isZero();
    }

    @Test
    public void shutdownWithNothingPendingPublishesNothing() {
        final BarrageSnapshotPerformanceStreamPublisher publisher = new BarrageSnapshotPerformanceStreamPublisher();
        final RecordingStreamConsumer consumer = new RecordingStreamConsumer();
        publisher.register(consumer);

        publisher.shutdown();

        assertThat(consumer.batchCount()).isZero();
        publisher.add("id", "key", 1L, 1L, 1L, 1L, 1L);
        publisher.flush();
        assertThat(consumer.batchCount()).isZero();
    }
}
