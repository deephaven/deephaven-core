package io.deephaven.kafka.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.kafka.KafkaTools;
import io.deephaven.streampublisher.KeyOrValueIngestData;
import io.deephaven.streampublisher.KeyOrValueSpec;
import io.deephaven.streampublisher.json.JsonConsumeImpl;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class KafkaJsonConsumeImpl extends JsonConsumeImpl implements KafkaTools.Consume.KeyOrValueSpec {
    public KafkaJsonConsumeImpl(@NotNull ColumnDefinition<?>[] columnDefinitions, @Nullable Map<String, String> fieldNameToColumnName, @Nullable ObjectMapper objectMapper) {
        super(columnDefinitions, fieldNameToColumnName, objectMapper);
    }

    @Override
    public Optional<SchemaProvider> getSchemaProvider() {
        return Optional.empty();
    }

    @Override
    public Deserializer<?> getDeserializer(KafkaTools.KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                                              Map<String, ?> configs) {
        return new StringDeserializer();
    }

    @Override
    public KeyOrValueIngestData getIngestData(KafkaTools.KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
        final KeyOrValueSpec.KeyOrValue keyOrValue1 = keyOrValue == KafkaTools.KeyOrValue.KEY ? KeyOrValueSpec.KeyOrValue.KEY : KeyOrValueSpec.KeyOrValue.VALUE;
        return super.getIngestData(keyOrValue1, configs, nextColumnIndexMut, columnDefinitionsOut);
    }
}
