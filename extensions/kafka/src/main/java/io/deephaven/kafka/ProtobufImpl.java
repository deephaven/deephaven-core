/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.ColumnName;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToPrimitiveFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.FieldCopierAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions.FieldPathToColumnName;
import io.deephaven.protobuf.FieldNumberPath;
import io.deephaven.protobuf.FieldOptions;
import io.deephaven.protobuf.FieldPath;
import io.deephaven.protobuf.ProtobufDescriptorParser;
import io.deephaven.protobuf.ProtobufDescriptorParserOptions;
import io.deephaven.protobuf.ProtobufFunction;
import io.deephaven.protobuf.ProtobufFunctions;
import io.deephaven.protobuf.ProtobufFunctions.Builder;
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.qst.type.Type.Visitor;
import io.deephaven.util.annotations.VisibleForTesting;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

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
    static ProtobufFunctions schemaChangeAwareFunctions(Descriptor descriptor,
            ProtobufDescriptorParserOptions options) {
        return new ParsedStates(descriptor, options).functionsForSchemaChanges();
    }

    static final class ProtobufConsumeImpl extends Consume.KeyOrValueSpec {

        private static final ToObjectFunction<Object, Message> PROTOBUF_MESSAGE_OBJ =
                ToObjectFunction.identity(Type.ofCustom(Message.class));

        private final ProtobufConsumeOptions specs;

        ProtobufConsumeImpl(ProtobufConsumeOptions specs) {
            this.specs = Objects.requireNonNull(specs);
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.of(new ProtobufSchemaProvider());
        }

        @Override
        protected Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return new KafkaProtobufDeserializer<>(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        protected KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs, MutableInt nextColumnIndexMut, List<ColumnDefinition<?>> columnDefinitionsOut) {
            final Descriptor descriptor;
            try {
                descriptor = getDescriptor(schemaRegistryClient);
            } catch (RestClientException | IOException e) {
                throw new UncheckedDeephavenException(e);
            }
            final ProtobufFunctions functions = schemaChangeAwareFunctions(descriptor, specs.parserOptions());
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

        private Descriptor getDescriptor(SchemaRegistryClient schemaRegistryClient)
                throws RestClientException, IOException {
            final SchemaMetadata metadata = specs.schemaVersion().isPresent()
                    ? schemaRegistryClient.getSchemaMetadata(specs.schemaSubject(), specs.schemaVersion().getAsInt())
                    : schemaRegistryClient.getLatestSchemaMetadata(specs.schemaSubject());
            if (!ProtobufSchema.TYPE.equals(metadata.getSchemaType())) {
                throw new IllegalStateException(String.format("Expected schema type %s but was %s", ProtobufSchema.TYPE,
                        metadata.getSchemaType()));
            }
            final ProtobufSchema protobufSchema = (ProtobufSchema) schemaRegistryClient
                    .getSchemaBySubjectAndId(specs.schemaSubject(), metadata.getId());
            // The potential need to set io.deephaven.kafka.protobuf.ProtobufConsumeOptions#schemaMessageName
            // seems unfortunate; I'm surprised the information is not part of the kafka serdes protocol.
            // Maybe it's so that a single schema can be used, and different topics with different root messages can
            // all share that common schema?
            return specs.schemaMessageName().isPresent()
                    ? protobufSchema.toDescriptor(specs.schemaMessageName().get())
                    : protobufSchema.toDescriptor();
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

    private static class ParsedStates {
        private final Descriptor originalDescriptor;
        private final ProtobufDescriptorParserOptions options;
        private final Map<Descriptor, ProtobufFunctions> parsed;

        private ParsedStates(Descriptor originalDescriptor, ProtobufDescriptorParserOptions options) {
            this.originalDescriptor = Objects.requireNonNull(originalDescriptor);
            this.options = Objects.requireNonNull(options);
            this.parsed = new HashMap<>();
            getOrCreate(originalDescriptor);
        }

        public ProtobufFunctions functionsForSchemaChanges() {
            final Builder builder = ProtobufFunctions.builder();
            for (ProtobufFunction f : getOrCreate(originalDescriptor).functions()) {
                builder.addFunctions(ProtobufFunction.of(f.path(), new ForPath(f).adaptForSchemaChanges()));
            }
            return builder.build();
        }

        private ProtobufFunctions getOrCreate(Descriptor descriptor) {
            return parsed.computeIfAbsent(descriptor, this::create);
        }

        private ProtobufFunctions create(Descriptor newDescriptor) {
            if (!originalDescriptor.getFullName().equals(newDescriptor.getFullName())) {
                throw new IllegalArgumentException(String.format(
                        "Expected descriptor names to match. expected='%s', actual='%s'. You may need to explicitly set schema_message_name.",
                        originalDescriptor.getFullName(), newDescriptor.getFullName()));
            }
            if (newDescriptor == originalDescriptor) {
                return withMostAppropriateType(ProtobufDescriptorParser.parse(newDescriptor, options));
            }
            final Function<FieldPath, FieldOptions> adaptedOptions = fieldPath -> {
                final FieldPath originalFieldPath = adaptFieldPath(fieldPath).orElse(null);
                if (originalFieldPath == null) {
                    // This must be a new field, exclude it.
                    return FieldOptions.exclude();
                }
                return options.fieldOptions().apply(originalFieldPath);
            };
            final ProtobufDescriptorParserOptions a = ProtobufDescriptorParserOptions.builder()
                    .parsers(options.parsers())
                    .fieldOptions(adaptedOptions)
                    .build();
            return withMostAppropriateType(ProtobufDescriptorParser.parse(newDescriptor, a));
        }

        private Optional<FieldPath> adaptFieldPath(FieldPath path) {
            if (path.path().isEmpty()) {
                return Optional.of(path);
            }
            final List<FieldDescriptor> originalFds = new ArrayList<>(path.path().size());
            Descriptor descriptor = originalDescriptor;
            final Iterator<FieldDescriptor> it = path.path().iterator();
            while (true) {
                final FieldDescriptor currentFd = it.next();
                final FieldDescriptor originalFd = descriptor.findFieldByNumber(currentFd.getNumber());
                if (originalFd == null) {
                    // originalFd does not exist
                    return Optional.empty();
                }
                originalFds.add(originalFd);
                if (!it.hasNext()) {
                    break;
                }
                descriptor = originalFd.getMessageType();
            }
            return Optional.of(FieldPath.of(originalFds));
        }

        private class ForPath {
            private final ProtobufFunction originalFunction;
            private final Map<Descriptor, TypedFunction<Message>> functions;

            public ForPath(ProtobufFunction originalFunction) {
                this.originalFunction = Objects.requireNonNull(originalFunction);
                this.functions = new HashMap<>();
            }

            public TypedFunction<Message> adaptForSchemaChanges() {
                final Type<?> originalReturnType = originalReturnType();
                final TypedFunction<Message> out = originalReturnType.walk(new AdaptForSchemaChanges());
                if (!originalReturnType.equals(out.returnType())) {
                    throw new IllegalStateException(String.format(
                            "AdaptForSchemaChanges error, mismatched types for %s. expected=%s, actual=%s",
                            originalFunction.path().namePath(), originalReturnType, out.returnType()));
                }
                return out;
            }

            private Type<?> originalReturnType() {
                return originalFunction.function().returnType();
            }

            private boolean test(Message message) {
                return ((ToBooleanFunction<Message>) getOrCreateForType(message)).test(message);
            }

            private char applyAsChar(Message message) {
                return ((ToCharFunction<Message>) getOrCreateForType(message)).applyAsChar(message);
            }

            private byte applyAsByte(Message message) {
                return ((ToByteFunction<Message>) getOrCreateForType(message)).applyAsByte(message);
            }

            private short applyAsShort(Message message) {
                return ((ToShortFunction<Message>) getOrCreateForType(message)).applyAsShort(message);
            }

            private int applyAsInt(Message message) {
                return ((ToIntFunction<Message>) getOrCreateForType(message)).applyAsInt(message);
            }

            private long applyAsLong(Message message) {
                return ((ToLongFunction<Message>) getOrCreateForType(message)).applyAsLong(message);
            }

            private float applyAsFloat(Message message) {
                return ((ToFloatFunction<Message>) getOrCreateForType(message)).applyAsFloat(message);
            }

            private double applyAsDouble(Message message) {
                return ((ToDoubleFunction<Message>) getOrCreateForType(message)).applyAsDouble(message);
            }

            private <T> T applyAsObject(Message message) {
                return ((ToObjectFunction<Message, T>) getOrCreateForType(message)).apply(message);
            }

            private TypedFunction<Message> getOrCreateForType(Message message) {
                return getOrCreate(message.getDescriptorForType());
            }

            private TypedFunction<Message> getOrCreate(Descriptor descriptor) {
                return functions.computeIfAbsent(descriptor, this::createFunctionFor);
            }

            private TypedFunction<Message> createFunctionFor(Descriptor descriptor) {
                final Type<?> originalReturnType = originalReturnType();
                final TypedFunction<Message> newFunction =
                        find(ParsedStates.this.getOrCreate(descriptor), originalFunction.path().numberPath())
                                .map(ProtobufFunction::function)
                                .orElse(null);
                final TypedFunction<Message> adaptedFunction =
                        SchemaChangeAdaptFunction.of(newFunction, originalReturnType).orElse(null);
                if (adaptedFunction == null) {
                    throw new UncheckedDeephavenException(
                            String.format("Incompatible schema change for %s, originalType=%s, newType=%s",
                                    originalFunction.path().namePath(), originalReturnType,
                                    newFunction == null ? null : newFunction.returnType()));
                }
                if (!originalReturnType.equals(adaptedFunction.returnType())) {
                    // If this happens, must be a logical error in SchemaChangeAdaptFunction
                    throw new IllegalStateException(String.format(
                            "Expected adapted return types to be equal for %s, originalType=%s, adaptedType=%s",
                            originalFunction.path().namePath(), originalReturnType, adaptedFunction.returnType()));
                }
                return adaptedFunction;
            }

            class AdaptForSchemaChanges
                    implements Visitor<TypedFunction<Message>>, PrimitiveType.Visitor<TypedFunction<Message>> {

                @Override
                public TypedFunction<Message> visit(PrimitiveType<?> primitiveType) {
                    return primitiveType.walk((PrimitiveType.Visitor<TypedFunction<Message>>) this);
                }

                @Override
                public ToObjectFunction<Message, Object> visit(GenericType<?> genericType) {
                    // noinspection unchecked
                    return ToObjectFunction.of(ForPath.this::applyAsObject, (GenericType<Object>) genericType);
                }

                @Override
                public ToBooleanFunction<Message> visit(BooleanType booleanType) {
                    return ForPath.this::test;
                }

                @Override
                public ToByteFunction<Message> visit(ByteType byteType) {
                    return ForPath.this::applyAsByte;
                }

                @Override
                public ToCharFunction<Message> visit(CharType charType) {
                    return ForPath.this::applyAsChar;
                }

                @Override
                public ToShortFunction<Message> visit(ShortType shortType) {
                    return ForPath.this::applyAsShort;
                }

                @Override
                public ToIntFunction<Message> visit(IntType intType) {
                    return ForPath.this::applyAsInt;
                }

                @Override
                public ToLongFunction<Message> visit(LongType longType) {
                    return ForPath.this::applyAsLong;
                }

                @Override
                public ToFloatFunction<Message> visit(FloatType floatType) {
                    return ForPath.this::applyAsFloat;
                }

                @Override
                public ToDoubleFunction<Message> visit(DoubleType doubleType) {
                    return ForPath.this::applyAsDouble;
                }
            }
        }
    }

    private static class SchemaChangeAdaptFunction<T> implements TypedFunction.Visitor<T, TypedFunction<T>> {

        public static <T> Optional<TypedFunction<T>> of(TypedFunction<T> f, Type<?> desiredReturnType) {
            if (f == null) {
                return NullFunctions.of(desiredReturnType);
            }
            if (desiredReturnType.equals(f.returnType())) {
                return Optional.of(f);
            }
            return Optional.ofNullable(f.walk(new SchemaChangeAdaptFunction<>(desiredReturnType)));
        }

        private final Type<?> desiredReturnType;

        public SchemaChangeAdaptFunction(Type<?> desiredReturnType) {
            this.desiredReturnType = Objects.requireNonNull(desiredReturnType);
        }

        @Override
        public TypedFunction<T> visit(ToPrimitiveFunction<T> f) {
            if (desiredReturnType.equals(f.returnType().boxedType())) {
                return BoxTransform.of(f);
            }
            return null;
        }

        @Override
        public TypedFunction<T> visit(ToObjectFunction<T, ?> f) {
            return f.returnType().walk(new GenericType.Visitor<>() {
                @Override
                public TypedFunction<T> visit(BoxedType<?> boxedType) {
                    if (desiredReturnType.equals(boxedType.primitiveType())) {
                        return UnboxTransform.of(f).orElse(null);
                    }
                    return null;
                }

                @Override
                public TypedFunction<T> visit(StringType stringType) {
                    return null;
                }

                @Override
                public TypedFunction<T> visit(InstantType instantType) {
                    return null;
                }

                @Override
                public TypedFunction<T> visit(ArrayType<?, ?> arrayType) {
                    return null;
                }

                @Override
                public TypedFunction<T> visit(CustomType<?> customType) {
                    return null;
                }
            });
        }
    }

    private static Optional<ProtobufFunction> find(ProtobufFunctions f, FieldNumberPath numberPath) {
        for (ProtobufFunction function : f.functions()) {
            if (numberPath.equals(function.path().numberPath())) {
                return Optional.of(function);
            }
        }
        return Optional.empty();
    }
}
