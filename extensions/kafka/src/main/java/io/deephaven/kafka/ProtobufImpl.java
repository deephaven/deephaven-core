//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToPrimitiveFunction;
import io.deephaven.function.TypedFunction;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.FieldCopierAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.kafka.protobuf.DescriptorMessageClass;
import io.deephaven.kafka.protobuf.DescriptorProvider;
import io.deephaven.kafka.protobuf.DescriptorSchemaRegistry;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions.FieldPathToColumnName;
import io.deephaven.protobuf.FieldPath;
import io.deephaven.protobuf.ProtobufDescriptorParser;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import io.deephaven.protobuf.ProtobufFunction;
import io.deephaven.protobuf.ProtobufFunctions;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.Type;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * This layer builds on top of {@link ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions)} by
 * further transforming the functions according to {@link #withMostAppropriateType(TypedFunction)}, and then further
 * adapting the functions to ensure they work for the expected chunk types {@link ToChunkTypeTransform}. This layer is
 * also responsible for managing schema changes; in essence, ensuring that newly
 * {@link ProtobufDescriptorParser#parse(Descriptor, ProtobufDescriptorParserOptions) parsed} {@link Descriptor
 * descriptor} {@link TypedFunction functions} can be adapted into the original function type.
 */
class ProtobufImpl {

    @VisibleForTesting
    static ProtobufFunctions simple(Descriptor descriptor, ProtobufDescriptorParserOptions options) {
        return withMostAppropriateType(ProtobufDescriptorParser.parse(descriptor, options));
    }

    static final class ProtobufConsumeImpl extends Consume.KeyOrValueSpec {

        private static final ToObjectFunction<Object, Message> PROTOBUF_MESSAGE_OBJ =
                ToObjectFunction.identity(Type.ofCustom(Message.class));

        private final ProtobufConsumeOptions specs;
        private Descriptor descriptor;

        ProtobufConsumeImpl(ProtobufConsumeOptions specs) {
            this.specs = Objects.requireNonNull(specs);
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return specs.descriptorProvider() instanceof DescriptorSchemaRegistry
                    ? Optional.of(new ProtobufSchemaProvider())
                    : Optional.empty();
        }

        @Override
        protected Deserializer<? extends Message> getDeserializer(
                KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            final DescriptorProvider dp = specs.descriptorProvider();
            if (dp instanceof DescriptorMessageClass) {
                setDescriptor((DescriptorMessageClass<?>) dp);
                return deserializer((DescriptorMessageClass<?>) dp);
            }
            if (dp instanceof DescriptorSchemaRegistry) {
                setDescriptor(schemaRegistryClient, (DescriptorSchemaRegistry) dp);
                return deserializer((DescriptorSchemaRegistry) dp);
            }
            throw new IllegalStateException("Unexpected descriptor provider: " + dp);
        }

        private void setDescriptor(DescriptorMessageClass<?> dmc) {
            try {
                descriptor = descriptor(dmc.clazz());
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new UncheckedDeephavenException(e);
            }
        }

        private void setDescriptor(SchemaRegistryClient schemaRegistryClient, DescriptorSchemaRegistry dsr) {
            try {
                descriptor = descriptor(schemaRegistryClient, dsr);
            } catch (RestClientException | IOException e) {
                throw new UncheckedDeephavenException(e);
            }
        }

        private Deserializer<? extends Message> deserializer(DescriptorMessageClass<?> dmc) {
            final Parser<? extends Message> parser;
            try {
                parser = parser(dmc.clazz());
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new UncheckedDeephavenException(e);
            }
            return ProtobufDeserializers.of(specs.protocol(), parser);
        }

        private Deserializer<DynamicMessage> deserializer(@SuppressWarnings("unused") DescriptorSchemaRegistry dsr) {
            // Note: taking in an unused DescriptorSchemaRegistry to re-enforce that this path should only be used in
            // the case where we are getting a the descriptor from the schema registry.
            return ProtobufDeserializers.of(specs.protocol(), descriptor);
        }

        @Override
        protected KeyOrValueIngestData getIngestData(
                KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs,
                MutableInt nextColumnIndexMut,
                List<ColumnDefinition<?>> columnDefinitionsOut) {
            // Given our deserializer setup above, we are guaranteeing that all returned messages will have the exact
            // same descriptor. This simplifies the logic we need to construct appropriate ProtobufFunctions.
            final ProtobufFunctions functions = simple(descriptor, specs.parserOptions());
            final List<FieldCopier> fieldCopiers = new ArrayList<>(functions.functions().size());
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.fieldPathToColumnName = new LinkedHashMap<>();
            final FieldPathToColumnName fieldPathToColumnName = specs.pathToColumnName();
            final Map<FieldPath, Integer> indices = new HashMap<>();
            for (ProtobufFunction f : functions.functions()) {
                final int ix = indices.compute(f.path(), (fieldPath, i) -> i == null ? 0 : i + 1);
                final ColumnName columnName = fieldPathToColumnName.columnName(f.path(), ix);
                add(columnName, f.function(), data, columnDefinitionsOut, fieldCopiers);
            }
            // we don't have enough info at this time to create KeyOrValueProcessorImpl
            // data.extra = new KeyOrValueProcessorImpl(MultiFieldChunkAdapter.chunkOffsets(null, null), fieldCopiers,
            // false);
            data.extra = fieldCopiers;
            return data;
        }

        private void add(
                ColumnName columnName,
                TypedFunction<Message> function,
                KeyOrValueIngestData data,
                List<ColumnDefinition<?>> columnDefinitionsOut,
                List<FieldCopier> fieldCopiersOut) {
            data.fieldPathToColumnName.put(columnName.name(), columnName.name());
            columnDefinitionsOut.add(ColumnDefinition.of(columnName.name(), function.returnType()));
            fieldCopiersOut.add(FieldCopierAdapter.of(PROTOBUF_MESSAGE_OBJ.map(ToChunkTypeTransform.of(function))));
        }

        @Override
        protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            // noinspection unchecked
            return new KeyOrValueProcessorImpl(
                    MultiFieldChunkAdapter.chunkOffsets(tableDef, data.fieldPathToColumnName),
                    (List<FieldCopier>) data.extra, false);
        }
    }

    private static ProtobufFunctions withMostAppropriateType(ProtobufFunctions functions) {
        final Builder builder = ProtobufFunctions.builder();
        for (ProtobufFunction f : functions.functions()) {
            builder.addFunctions(ProtobufFunction.of(f.path(), withMostAppropriateType(f.function())));
        }
        return builder.build();
    }

    /**
     * Adapts {@code f} to the most appropriate Deephaven equivalent / nullable type.
     *
     * <ul>
     * <li>boolean -> Boolean</li>
     * <li>Byte -> byte</li>
     * <li>Character -> char</li>
     * <li>Short -> short</li>
     * <li>Integer -> int</li>
     * <li>Long -> long</li>
     * <li>Float -> float</li>
     * <li>Double -> double</li>
     * </ul>
     */
    private static <X> TypedFunction<X> withMostAppropriateType(TypedFunction<X> f) {
        final TypedFunction<X> f2 = DhNullableTypeTransform.of(f);
        final ToPrimitiveFunction<X> unboxed = UnboxTransform.of(f2).orElse(null);
        return unboxed != null ? unboxed : f2;
    }

    private static Descriptor descriptor(Class<? extends Message> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method getDescriptor = clazz.getMethod("getDescriptor");
        return (Descriptor) getDescriptor.invoke(null);
    }

    private static Descriptor descriptor(SchemaRegistryClient registry, DescriptorSchemaRegistry dsr)
            throws RestClientException, IOException {
        final SchemaMetadata metadata = dsr.version().isPresent()
                ? registry.getSchemaMetadata(dsr.subject(), dsr.version().getAsInt())
                : registry.getLatestSchemaMetadata(dsr.subject());
        if (!ProtobufSchema.TYPE.equals(metadata.getSchemaType())) {
            throw new IllegalStateException(String.format("Expected schema type %s but was %s", ProtobufSchema.TYPE,
                    metadata.getSchemaType()));
        }
        final ProtobufSchema protobufSchema = (ProtobufSchema) registry
                .getSchemaBySubjectAndId(dsr.subject(), metadata.getId());
        // The potential need to set io.deephaven.kafka.protobuf.DescriptorSchemaRegistry#messageName
        // seems unfortunate; I'm surprised the information is not part of the kafka serdes protocol.
        // Maybe it's so that a single schema can be used, and different topics with different root messages can
        // all share that common schema?
        return dsr.messageName().isPresent()
                ? protobufSchema.toDescriptor(dsr.messageName().get())
                : protobufSchema.toDescriptor();
    }

    private static <T extends Message> Parser<T> parser(Class<T> clazz)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        final Method parser = clazz.getMethod("parser");
        // noinspection unchecked
        return (Parser<T>) parser.invoke(null);
    }
}
