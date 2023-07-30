/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.DataFormat;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Objects;

class RawImpl {

    static final class RawConsume extends Consume.KeyOrValueSpec {
        final ColumnDefinition<?> cd;
        final Class<? extends Deserializer<?>> deserializer;

        public RawConsume(ColumnDefinition<?> cd, Class<? extends Deserializer<?>> deserializer) {
            this.cd = Objects.requireNonNull(cd);
            this.deserializer = Objects.requireNonNull(deserializer);
        }

        @Override
        DataFormat dataFormat() {
            return DataFormat.RAW;
        }
    }

    static final class RawProduce extends KeyOrValueSpec {
        final String columnName;
        final Class<? extends Serializer<?>> serializer;

        public RawProduce(String columnName, Class<? extends Serializer<?>> serializer) {
            this.columnName = Objects.requireNonNull(columnName);
            this.serializer = Objects.requireNonNull(serializer);
        }

        @Override
        DataFormat dataFormat() {
            return DataFormat.RAW;
        }
    }
}
