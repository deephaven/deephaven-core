package io.deephaven.kafka;

import io.deephaven.qst.type.Type;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.Test;

import java.util.Map.Entry;

import static org.assertj.core.api.Assertions.assertThat;

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
}
