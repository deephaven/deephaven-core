/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.DataFormat;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;

class SimpleImpl {
    /**
     * Single spec for unidimensional (basic Kafka encoded for one type) fields.
     */
    static final class SimpleConsume extends Consume.KeyOrValueSpec {
        final String columnName;
        final Class<?> dataType;

        SimpleConsume(final String columnName, final Class<?> dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
        }

        @Override
        DataFormat dataFormat() {
            return DataFormat.SIMPLE;
        }
    }

    /**
     * Single spec for unidimensional (basic Kafka encoded for one type) fields.
     */
    static final class SimpleProduce extends KeyOrValueSpec {
        final String columnName;

        SimpleProduce(final String columnName) {
            this.columnName = columnName;
        }

        @Override
        DataFormat dataFormat() {
            return DataFormat.SIMPLE;
        }
    }
}
