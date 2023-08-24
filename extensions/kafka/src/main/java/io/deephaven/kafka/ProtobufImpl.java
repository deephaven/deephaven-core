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
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.FieldCopier;
import io.deephaven.kafka.ingest.FieldCopierAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.ingest.MultiFieldChunkAdapter;
import io.deephaven.kafka.protobuf.ProtobufConsumeOptions;
import io.deephaven.protobuf.FieldNumberPath;
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
import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.PrimitiveFunction;
import io.deephaven.functions.ShortFunction;
import io.deephaven.functions.TypedFunction;
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
import java.util.stream.Collectors;

class ProtobufImpl {

    @VisibleForTesting
    static ProtobufFunctions schemaChangeAwareFunctions(Descriptor descriptor,
            ProtobufDescriptorParserOptions options) {
        return new ParsedStates(descriptor, options).functionsForSchemaChanges();
    }

    static final class ProtobufConsumeImpl extends Consume.KeyOrValueSpec {

        private static final ObjectFunction<Object, Message> PROTOBUF_MESSAGE_OBJ =
                ObjectFunction.identity(Type.ofCustom(Message.class));

        private final ProtobufConsumeOptions specs;

        ProtobufConsumeImpl(ProtobufConsumeOptions specs) {
            this.specs = Objects.requireNonNull(specs);
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.of(new ProtobufSchemaProvider());
        }

        @Override
        Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return new KafkaProtobufDeserializer<>(Objects.requireNonNull(schemaRegistryClient));
        }

        @Override
        KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
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
            // arguably, others should be LinkedHashMap as well.
            data.fieldPathToColumnName = new LinkedHashMap<>();
            final Function<FieldPath, String> pathToColumnName = specs.pathToColumnName();
            for (ProtobufFunction f : functions.functions()) {
                add(pathToColumnName.apply(f.path()), f.function(), data, columnDefinitionsOut, fieldCopiers);
            }
            // we don't have enough info at this time to create KeyOrValueProcessorImpl
            // data.extra = new KeyOrValueProcessorImpl(MultiFieldChunkAdapter.chunkOffsets(null, null), fieldCopiers,
            // false);
            data.extra = fieldCopiers;
            return data;
        }

        private void add(
                String columnName,
                TypedFunction<Message> function,
                KeyOrValueIngestData data,
                List<ColumnDefinition<?>> columnDefinitionsOut,
                List<FieldCopier> fieldCopiersOut) {
            data.fieldPathToColumnName.put(columnName, columnName);
            columnDefinitionsOut.add(ColumnDefinition.of(columnName, function.returnType()));
            fieldCopiersOut.add(FieldCopierAdapter.of(PROTOBUF_MESSAGE_OBJ.map(CommonTransform.of(function))));
        }

        @Override
        KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
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
            // todo: we need to handle the dynamic case eventually, where protobuf descriptor is updated
            return ((ProtobufSchema) schemaRegistryClient.getSchemaBySubjectAndId(specs.schemaSubject(),
                    metadata.getId()))
                    .toDescriptor();
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
        final PrimitiveFunction<X> unboxed = UnboxTransform.of(f2).orElse(null);
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
                        "Expected descriptor names to match. originalDescriptor.getFullName()=%s, newDescriptor.getFullName()=%s",
                        originalDescriptor.getFullName(), newDescriptor.getFullName()));
            }
            if (newDescriptor == originalDescriptor) {
                return withMostAppropriateType(ProtobufDescriptorParser.parse(newDescriptor, options));
            }
            // We only need to include the field numbers that were parsed for the original descriptor
            final List<FieldNumberPath> includePaths = parsed.get(originalDescriptor)
                    .functions()
                    .stream()
                    .map(ProtobufFunction::path)
                    .map(FieldPath::numberPath)
                    .collect(Collectors.toList());
            // While we could do
            // .include(BooleanFunction.ofTrue()) or .include(adapt(options.include()))
            // the version we are using
            // .include(FieldPath.anyNumberPathStartsWithUs(includePaths))
            // is more efficient / selective.
            final ProtobufDescriptorParserOptions adaptedOptions = ProtobufDescriptorParserOptions.builder()
                    .parsers(options.parsers())
                    .include(FieldPath.anyNumberPathStartsWithUs(includePaths))
                    .parseAsWellKnown(adapt(options.parseAsWellKnown()))
                    .parseAsBytes(adapt(options.parseAsBytes()))
                    .parseAsMap(adapt(options.parseAsMap()))
                    .build();
            return withMostAppropriateType(ProtobufDescriptorParser.parse(newDescriptor, adaptedOptions));
        }

        private BooleanFunction<FieldPath> adapt(BooleanFunction<FieldPath> original) {
            if (original == BooleanFunction.<FieldPath>ofTrue() || original == BooleanFunction.<FieldPath>ofFalse()) {
                // Don't adapt FieldPath, original doesn't use FieldPath.
                return original;
            }
            return original.mapInput(this::adaptFieldPath);
        }

        private FieldPath adaptFieldPath(FieldPath path) {
            if (path.path().isEmpty()) {
                return path;
            }
            final List<FieldDescriptor> originalFds = new ArrayList<>(path.path().size());
            Descriptor descriptor = originalDescriptor;
            final Iterator<FieldDescriptor> it = path.path().iterator();
            while (true) {
                final FieldDescriptor currentFd = it.next();
                final FieldDescriptor originalFd = descriptor.findFieldByNumber(currentFd.getNumber());
                originalFds.add(originalFd);
                if (!it.hasNext()) {
                    break;
                }
                descriptor = originalFd.getMessageType();
            }
            return FieldPath.of(originalFds);
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
                return ((BooleanFunction<Message>) getOrCreateForType(message)).test(message);
            }

            private char applyAsChar(Message message) {
                return ((CharFunction<Message>) getOrCreateForType(message)).applyAsChar(message);
            }

            private byte applyAsByte(Message message) {
                return ((ByteFunction<Message>) getOrCreateForType(message)).applyAsByte(message);
            }

            private short applyAsShort(Message message) {
                return ((ShortFunction<Message>) getOrCreateForType(message)).applyAsShort(message);
            }

            private int applyAsInt(Message message) {
                return ((IntFunction<Message>) getOrCreateForType(message)).applyAsInt(message);
            }

            private long applyAsLong(Message message) {
                return ((LongFunction<Message>) getOrCreateForType(message)).applyAsLong(message);
            }

            private float applyAsFloat(Message message) {
                return ((FloatFunction<Message>) getOrCreateForType(message)).applyAsFloat(message);
            }

            private double applyAsDouble(Message message) {
                return ((DoubleFunction<Message>) getOrCreateForType(message)).applyAsDouble(message);
            }

            private <T> T applyAsObject(Message message) {
                return ((ObjectFunction<Message, T>) getOrCreateForType(message)).apply(message);
            }

            private TypedFunction<Message> getOrCreateForType(Message message) {
                return getOrCreate(message.getDescriptorForType());
            }

            private TypedFunction<Message> getOrCreate(Descriptor descriptor) {
                return functions.computeIfAbsent(descriptor, this::createFunctionFor);
            }

            private TypedFunction<Message> createFunctionFor(Descriptor descriptor) {
                final Type<?> originalReturnType = originalReturnType();
                final TypedFunction<Message> newFunction = ParsedStates.this.getOrCreate(descriptor)
                        .find(originalFunction.path().numberPath())
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
                public ObjectFunction<Message, Object> visit(GenericType<?> genericType) {
                    // noinspection unchecked
                    return ObjectFunction.of(ForPath.this::applyAsObject, (GenericType<Object>) genericType);
                }

                @Override
                public BooleanFunction<Message> visit(BooleanType booleanType) {
                    return ForPath.this::test;
                }

                @Override
                public ByteFunction<Message> visit(ByteType byteType) {
                    return ForPath.this::applyAsByte;
                }

                @Override
                public CharFunction<Message> visit(CharType charType) {
                    return ForPath.this::applyAsChar;
                }

                @Override
                public ShortFunction<Message> visit(ShortType shortType) {
                    return ForPath.this::applyAsShort;
                }

                @Override
                public IntFunction<Message> visit(IntType intType) {
                    return ForPath.this::applyAsInt;
                }

                @Override
                public LongFunction<Message> visit(LongType longType) {
                    return ForPath.this::applyAsLong;
                }

                @Override
                public FloatFunction<Message> visit(FloatType floatType) {
                    return ForPath.this::applyAsFloat;
                }

                @Override
                public DoubleFunction<Message> visit(DoubleType doubleType) {
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
        public TypedFunction<T> visit(PrimitiveFunction<T> f) {
            if (desiredReturnType.equals(f.returnType().boxedType())) {
                return BoxTransform.of(f);
            }
            return null;
        }

        @Override
        public TypedFunction<T> visit(ObjectFunction<T, ?> f) {
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
}
