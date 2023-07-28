/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.DataFormat;
import io.deephaven.kafka.KafkaTools.Produce.KeyOrValueSpec;

class IgnoreImpl {
    static final class IgnoreConsume extends Consume.KeyOrValueSpec {
        @Override
        DataFormat dataFormat() {
            return DataFormat.IGNORE;
        }
    }

    static final class IgnoreProduce extends KeyOrValueSpec {
        @Override
        DataFormat dataFormat() {
            return DataFormat.IGNORE;
        }
    }
}
