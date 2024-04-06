//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.SimpleImpl.SimpleConsume;
import io.deephaven.qst.type.Type;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Test;

import java.util.Map;
import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.assertj.core.api.Assertions.from;

public class SimpleImplTest {
    @Test
    public void deserializerConsistency() {
        for (Entry<String, Type<?>> e : SimpleImpl.DESER_NAME_TO_TYPE.entrySet()) {
            final String deserializerName = e.getKey();
            final Type<?> value = e.getValue();
            final Deserializer<?> deserializer = SimpleImpl.deserializer(value).orElse(null);
            assertThat(deserializer).isNotNull();
            assertThat(deserializer.getClass().getName()).isEqualTo(deserializerName);
        }
    }

    @Test
    public void noKeyType() {
        final SimpleConsume fromProperties = new SimpleConsume(null, null);
        try {
            fromProperties.getType(KeyOrValue.KEY, Map.of());
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (UncheckedDeephavenException e) {
            assertThat(e).hasMessage(
                    "Unable to find the type for column 'KafkaKey' (key_spec). Please explicitly set the data type in the constructor, or through the kafka configuration 'deephaven.key.column.type' or 'key.deserializer'.");
        }
    }

    @Test
    public void noValueType() {
        final SimpleConsume fromProperties = new SimpleConsume(null, null);
        try {
            fromProperties.getType(KeyOrValue.VALUE, Map.of());
            failBecauseExceptionWasNotThrown(UncheckedDeephavenException.class);
        } catch (UncheckedDeephavenException e) {
            assertThat(e).hasMessage(
                    "Unable to find the type for column 'KafkaValue' (value_spec). Please explicitly set the data type in the constructor, or through the kafka configuration 'deephaven.value.column.type' or 'value.deserializer'.");
        }
    }
}
