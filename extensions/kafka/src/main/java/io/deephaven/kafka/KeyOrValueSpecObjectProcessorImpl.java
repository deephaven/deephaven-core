//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume.KeyOrValueSpec;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.KafkaStreamPublisher;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.processor.ObjectProcessor;
import io.deephaven.qst.type.Type;
import io.deephaven.util.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * This implementation is useful for presenting an easier onboarding ramp and better (and public) interface
 * {@link KafkaTools.Consume#objectProcessorSpec(Deserializer, ObjectProcessor, List)} for end-users. The
 * {@link ObjectProcessor} is a user-visible replacement for {@link KeyOrValueProcessor}. In the meantime though, we are
 * adapting into a {@link KeyOrValueProcessor} until such a time when {@link KafkaStreamPublisher} can be re-written to
 * take advantage of these better interfaces.
 */
class KeyOrValueSpecObjectProcessorImpl<T> extends KeyOrValueSpec {
    private final Deserializer<? extends T> deserializer;
    private final ObjectProcessor<? super T> processor;
    private final List<String> columnNames;

    KeyOrValueSpecObjectProcessorImpl(
            Deserializer<? extends T> deserializer, ObjectProcessor<? super T> processor, List<String> columnNames) {
        if (columnNames.size() != processor.outputTypes().size()) {
            throw new IllegalArgumentException("Expected columnNames and processor.outputTypes() to be the same size");
        }
        if (columnNames.stream().distinct().count() != columnNames.size()) {
            throw new IllegalArgumentException("Expected columnNames to have distinct values");
        }
        this.deserializer = Objects.requireNonNull(deserializer);
        this.processor = Objects.requireNonNull(processor);
        this.columnNames = List.copyOf(columnNames);
    }

    @Override
    public Optional<SchemaProvider> getSchemaProvider() {
        return Optional.empty();
    }

    @Override
    protected Deserializer<? extends T> getDeserializer(KeyOrValue keyOrValue,
            SchemaRegistryClient schemaRegistryClient,
            Map<String, ?> configs) {
        return deserializer;
    }

    @Override
    protected KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
            Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        data.fieldPathToColumnName = new LinkedHashMap<>();
        final int L = columnNames.size();
        for (int i = 0; i < L; ++i) {
            final String columnName = columnNames.get(i);
            final Type<?> type = processor.outputTypes().get(i);
            data.fieldPathToColumnName.put(columnName, columnName);
            columnDefinitionsOut.add(ColumnDefinition.of(columnName, type));
        }
        return data;
    }

    @Override
    protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
        return new KeyOrValueProcessorImpl(
                offsetsFunction(MultiFieldChunkAdapter.chunkOffsets(tableDef, data.fieldPathToColumnName)));
    }

    private class KeyOrValueProcessorImpl implements KeyOrValueProcessor {
        private final Function<WritableChunk<?>[], List<WritableChunk<?>>> offsetsAdapter;

        private KeyOrValueProcessorImpl(Function<WritableChunk<?>[], List<WritableChunk<?>>> offsetsAdapter) {
            this.offsetsAdapter = Objects.requireNonNull(offsetsAdapter);
        }

        @Override
        public void handleChunk(ObjectChunk<Object, Values> inputChunk, WritableChunk<Values>[] publisherChunks) {
            // noinspection unchecked
            final ObjectChunk<T, ?> in = (ObjectChunk<T, ?>) inputChunk;
            // we except isInOrder to be true, so apply should be an O(1) op no matter how many columns there are.
            processor.processAll(in, offsetsAdapter.apply(publisherChunks));
        }
    }

    private static <T> Function<T[], List<T>> offsetsFunction(int[] offsets) {
        return offsets.length == 0
                ? array -> Collections.emptyList()
                : isInOrder(offsets)
                        ? array -> Arrays.asList(array).subList(offsets[0], offsets[0] + offsets.length)
                        : array -> reorder(array, offsets);
    }

    private static boolean isInOrder(int[] offsets) {
        for (int i = 1; i < offsets.length; ++i) {
            if (offsets[i - 1] + 1 != offsets[i]) {
                return false;
            }
        }
        return true;
    }

    private static <T> List<T> reorder(T[] array, int[] offsets) {
        final List<T> out = new ArrayList<>(offsets.length);
        for (int offset : offsets) {
            out.add(array[offset]);
        }
        return out;
    }
}
