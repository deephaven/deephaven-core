//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.palantir.javapoet.ArrayTypeName;
import com.palantir.javapoet.ClassName;
import com.palantir.javapoet.CodeBlock;
import com.palantir.javapoet.FieldSpec;
import com.palantir.javapoet.JavaFile;
import com.palantir.javapoet.MethodSpec;
import com.palantir.javapoet.ParameterSpec;
import com.palantir.javapoet.ParameterizedTypeName;
import com.palantir.javapoet.TypeName;
import com.palantir.javapoet.TypeSpec;
import com.palantir.javapoet.TypeVariableName;
import com.palantir.javapoet.WildcardTypeName;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.table.Context;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.ByteSortKernel;
import io.deephaven.engine.table.impl.sort.IntSortKernel;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.engine.table.impl.sort.timsort.indirect.IndirectTimsortKernelFactory;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.compare.ObjectComparisons;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Generates the timsort kernels for each sort value type, permutation value type, and sort direction using JavaPoet.
 *
 * <p>
 * The generator is organized around a list of {@link KeyColumn key columns} (the values compared by the sort) and an
 * optional permutation payload column (carried through the sort but never compared). Today every kernel has exactly one
 * key column, producing kernels functionally identical to the replicated kernels in
 * {@code io.deephaven.engine.table.impl.sort.timsort}; the key-column list is the extension point for generating
 * kernels that simultaneously sort multiple key columns of any type.
 */
public class GenerateTimsortKernels {
    private static final String PACKAGE = "io.deephaven.engine.table.impl.sort.timsort";
    private static final String INDIRECT_PACKAGE = PACKAGE + ".indirect";
    private static final File SOURCE_ROOT = new File("engine/table/src/main/java/");

    private static final ClassName MULTI_COLUMN_SORT_KERNEL = ClassName.get(MultiColumnSortKernel.class);
    private static final ClassName CHUNK_TYPE = ClassName.get(ChunkType.class);
    private static final ClassName SORTING_ORDER = ClassName.get(SortingOrder.class);

    private static final ClassName ANY = ClassName.get(Any.class);
    private static final ClassName CHUNK_POSITIONS = ClassName.get(ChunkPositions.class);
    private static final ClassName CHUNK_LENGTHS = ClassName.get(ChunkLengths.class);
    private static final ClassName INT_CHUNK = ClassName.get(IntChunk.class);
    private static final ClassName WRITABLE_CHUNK = ClassName.get(WritableChunk.class);
    private static final ClassName ENGINE_CONTEXT = ClassName.get(Context.class);
    private static final ClassName TIMSORT_UTILS = ClassName.get(TimsortUtils.class);
    private static final ClassName VISIBLE_FOR_TESTING = ClassName.get(VisibleForTesting.class);
    private static final ClassName COMPARATOR = ClassName.get(Comparator.class);

    /**
     * The type of chunk that holds a column's data; provides the chunk types for a given attribute and element type.
     */
    static final class ChunkFamily {
        final String name;
        final String plural;
        final TypeName elementType;
        final ClassName chunkName;
        final ClassName writableChunkName;
        final boolean isObject;

        ChunkFamily(final String name, final String plural, final TypeName elementType,
                final Class<?> chunkClass, final Class<?> writableChunkClass) {
            this.name = name;
            this.plural = plural;
            this.elementType = elementType;
            this.chunkName = ClassName.get(chunkClass);
            this.writableChunkName = ClassName.get(writableChunkClass);
            this.isObject = !elementType.isPrimitive();
        }

        TypeName chunkOf(final TypeName attr) {
            return isObject
                    ? ParameterizedTypeName.get(chunkName, ClassName.OBJECT, attr)
                    : ParameterizedTypeName.get(chunkName, attr);
        }

        TypeName writableChunkOf(final TypeName attr) {
            return isObject
                    ? ParameterizedTypeName.get(writableChunkName, ClassName.OBJECT, attr)
                    : ParameterizedTypeName.get(writableChunkName, attr);
        }

        TypeName chunkOfWildcard() {
            return chunkOf(WildcardTypeName.subtypeOf(ClassName.OBJECT));
        }

        TypeName writableChunkOfWildcard() {
            return writableChunkOf(WildcardTypeName.subtypeOf(ClassName.OBJECT));
        }

        String asWritableChunkMethod() {
            return "asWritable" + name + "Chunk";
        }
    }

    private static final ChunkFamily CHAR_CHUNKS =
            new ChunkFamily("Char", "Characters", TypeName.CHAR, CharChunk.class, WritableCharChunk.class);
    private static final ChunkFamily BYTE_CHUNKS =
            new ChunkFamily("Byte", "Bytes", TypeName.BYTE, ByteChunk.class, WritableByteChunk.class);
    private static final ChunkFamily SHORT_CHUNKS =
            new ChunkFamily("Short", "Shorts", TypeName.SHORT, ShortChunk.class, WritableShortChunk.class);
    private static final ChunkFamily INT_CHUNKS =
            new ChunkFamily("Int", "Integers", TypeName.INT, IntChunk.class, WritableIntChunk.class);
    private static final ChunkFamily LONG_CHUNKS =
            new ChunkFamily("Long", "Longs", TypeName.LONG, LongChunk.class, WritableLongChunk.class);
    private static final ChunkFamily FLOAT_CHUNKS =
            new ChunkFamily("Float", "Floats", TypeName.FLOAT, FloatChunk.class, WritableFloatChunk.class);
    private static final ChunkFamily DOUBLE_CHUNKS =
            new ChunkFamily("Double", "Doubles", TypeName.DOUBLE, DoubleChunk.class, WritableDoubleChunk.class);
    private static final ChunkFamily OBJECT_CHUNKS =
            new ChunkFamily("Object", "Objects", ClassName.OBJECT, ObjectChunk.class, WritableObjectChunk.class);

    enum Direction {
        ASCENDING, DESCENDING
    }

    /**
     * How a key column's values are compared. Emits the body of {@code doComparison} and knows whether the comparison
     * requires kernel instance state (as the {@link java.util.Comparator} based kernel does).
     */
    interface Comparison {
        CodeBlock comparisonExpression(Direction direction);

        default boolean requiresInstanceState() {
            return false;
        }
    }

    /** Comparison via a static two-argument compare method, e.g. {@code Character.compare(lhs, rhs)}. */
    static final class StaticCompare implements Comparison {
        private final ClassName compareClass;

        StaticCompare(final ClassName compareClass) {
            this.compareClass = compareClass;
        }

        @Override
        public CodeBlock comparisonExpression(final Direction direction) {
            if (direction == Direction.DESCENDING) {
                return CodeBlock.of("-1 * $T.compare(lhs, rhs)", compareClass);
            }
            return CodeBlock.of("$T.compare(lhs, rhs)", compareClass);
        }
    }

    /** Object comparison; descending swaps the arguments rather than negating, exactly as the replicated kernels. */
    static final class ObjectCompare implements Comparison {
        private final ClassName compareClass = ClassName.get(ObjectComparisons.class);

        @Override
        public CodeBlock comparisonExpression(final Direction direction) {
            if (direction == Direction.DESCENDING) {
                return CodeBlock.of("$T.compare(rhs, lhs)", compareClass);
            }
            return CodeBlock.of("$T.compare(lhs, rhs)", compareClass);
        }
    }

    /** Comparison through a {@link java.util.Comparator} held by the kernel instance. */
    static final class ComparatorCompare implements Comparison {
        @Override
        public CodeBlock comparisonExpression(final Direction direction) {
            if (direction == Direction.DESCENDING) {
                return CodeBlock.of("-1 * comparator.compare(lhs, rhs)");
            }
            return CodeBlock.of("comparator.compare(lhs, rhs)");
        }

        @Override
        public boolean requiresInstanceState() {
            return true;
        }
    }

    /**
     * The kind of a key column: its name fragment within the kernel class name, its chunk family, and its comparison.
     */
    static final class KeyKind {
        final String namePart;
        final ChunkFamily chunks;
        final Comparison comparison;

        KeyKind(final String namePart, final ChunkFamily chunks, final Comparison comparison) {
            this.namePart = namePart;
            this.chunks = chunks;
            this.comparison = comparison;
        }
    }

    /** The direct sort kernel interface implemented by kernels that permute the given family's values. */
    private static ClassName sortKernelInterface(final ChunkFamily permute) {
        if (permute == LONG_CHUNKS) {
            return ClassName.get(LongSortKernel.class);
        }
        if (permute == INT_CHUNKS) {
            return ClassName.get(IntSortKernel.class);
        }
        if (permute == BYTE_CHUNKS) {
            return ClassName.get(ByteSortKernel.class);
        }
        throw new IllegalArgumentException("no direct sort kernel interface permutes " + permute.name + " values");
    }

    private static KeyKind boxedKind(final ChunkFamily chunks, final Class<?> boxed) {
        return new KeyKind(chunks.name, chunks, new StaticCompare(ClassName.get(boxed)));
    }

    private static KeyKind comparisonsKind(final String namePart, final ChunkFamily chunks,
            final Class<?> comparisonsClass) {
        return new KeyKind(namePart, chunks, new StaticCompare(ClassName.get(comparisonsClass)));
    }

    private static final KeyKind CHAR_KIND = boxedKind(CHAR_CHUNKS, Character.class);
    private static final KeyKind BYTE_KIND = boxedKind(BYTE_CHUNKS, Byte.class);
    private static final KeyKind SHORT_KIND = boxedKind(SHORT_CHUNKS, Short.class);
    private static final KeyKind INT_KIND = boxedKind(INT_CHUNKS, Integer.class);
    private static final KeyKind LONG_KIND = boxedKind(LONG_CHUNKS, Long.class);
    private static final KeyKind FLOAT_KIND = comparisonsKind("Float", FLOAT_CHUNKS, FloatComparisons.class);
    private static final KeyKind DOUBLE_KIND = comparisonsKind("Double", DOUBLE_CHUNKS, DoubleComparisons.class);
    private static final KeyKind NULL_AWARE_CHAR_KIND =
            new KeyKind("NullAwareChar", CHAR_CHUNKS, new StaticCompare(ClassName.get(CharComparisons.class)));
    private static final KeyKind OBJECT_KIND = new KeyKind("Object", OBJECT_CHUNKS, new ObjectCompare());
    private static final KeyKind COMPARATOR_KIND = new KeyKind("Comparator", OBJECT_CHUNKS, new ComparatorCompare());

    /** A single sorted key column. Each key column may eventually carry its own direction. */
    static final class KeyColumn {
        final KeyKind kind;
        final Direction direction;

        KeyColumn(final KeyKind kind, final Direction direction) {
            this.kind = kind;
            this.direction = direction;
        }
    }

    /**
     * A kernel to generate: one or more key columns, plus an optional permutation payload chunk family (null for the
     * values-only kernels).
     */
    static final class KernelSpec {
        final List<KeyColumn> keys;
        final ChunkFamily permute;
        final Direction direction;

        KernelSpec(final List<KeyColumn> keys, final ChunkFamily permute, final Direction direction) {
            this.keys = keys;
            this.permute = permute;
            this.direction = direction;
        }

        KeyColumn singleKey() {
            if (keys.size() != 1) {
                throw new IllegalStateException("single-column emitter used for a multi-column spec");
            }
            return keys.get(0);
        }

        String packageName() {
            return PACKAGE;
        }

        private String keyNameParts() {
            final StringBuilder builder = new StringBuilder();
            for (final KeyColumn key : keys) {
                builder.append(key.kind.namePart);
            }
            return builder.toString();
        }

        String className() {
            final StringBuilder builder = new StringBuilder(keyNameParts());
            if (permute != null) {
                builder.append(permute.name);
            }
            builder.append(direction == Direction.DESCENDING ? "TimsortDescendingKernel" : "TimsortKernel");
            return builder.toString();
        }

        String contextName() {
            final StringBuilder builder = new StringBuilder(keyNameParts());
            if (permute != null) {
                builder.append(permute.name);
            }
            builder.append("SortKernelContext");
            return builder.toString();
        }
    }

    public static void main(String[] args) throws IOException {
        for (final KernelSpec spec : allSpecs()) {
            generate(spec);
        }
        generateIndirectKernels();
        generateIndirectDispatcher();
    }

    /** The engine column chunk types that have sort kernels; boolean columns are reinterpreted to bytes upstream. */
    private static final List<ChunkType> ENGINE_CHUNK_TYPES = List.of(ChunkType.Char, ChunkType.Byte, ChunkType.Short,
            ChunkType.Int, ChunkType.Long, ChunkType.Float, ChunkType.Double, ChunkType.Object);

    /**
     * Pregenerates the single-column indirect kernels for every engine column type in both directions, plus the
     * single-column comparator kernel, delegating to the same IndirectTimsortKernelFactory emitter that compiles the
     * multi-column kernels on demand at runtime.
     */
    private static void generateIndirectKernels() throws IOException {
        for (final ChunkType chunkType : ENGINE_CHUNK_TYPES) {
            for (final SortingOrder order : SortingOrder.values()) {
                final ChunkType[] chunkTypes = {chunkType};
                final SortingOrder[] orders = {order};
                writeFile(INDIRECT_PACKAGE,
                        IndirectTimsortKernelFactory.generateKernelType(chunkTypes, orders, new boolean[1],
                                INDIRECT_PACKAGE),
                        IndirectTimsortKernelFactory.kernelName(chunkTypes, orders, new boolean[1]));
            }
        }
        // the comparator kernel always compares in the ascending sense; descending comparator sorts reverse the
        // comparator when the context is created, so only one kernel is needed
        final ChunkType[] objectChunkTypes = {ChunkType.Object};
        final SortingOrder[] ascending = {SortingOrder.Ascending};
        final boolean[] hasComparator = {true};
        writeFile(INDIRECT_PACKAGE,
                IndirectTimsortKernelFactory.generateKernelType(objectChunkTypes, ascending, hasComparator,
                        INDIRECT_PACKAGE),
                IndirectTimsortKernelFactory.kernelName(objectChunkTypes, ascending, hasComparator));
    }

    /**
     * Emits the dispatcher that selects a pregenerated single-column indirect kernel by chunk type, sort direction, and
     * comparator. Multi-column shapes are not pregenerated; IndirectTimsortKernelFactory compiles them on demand.
     */
    private static void generateIndirectDispatcher() throws IOException {
        final TypeVariableName permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);

        final MethodSpec.Builder makeContext = MethodSpec.methodBuilder("makeContext")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addTypeVariable(permuteAttr)
                .returns(ParameterizedTypeName.get(MULTI_COLUMN_SORT_KERNEL, permuteAttr))
                .addParameter(ArrayTypeName.of(CHUNK_TYPE), "chunkTypes")
                .addParameter(ArrayTypeName.of(SORTING_ORDER), "order")
                .addParameter(ArrayTypeName.of(COMPARATOR), "comparators")
                .addParameter(int.class, "size");

        makeContext.beginControlFlow("if (chunkTypes.length != 1)");
        makeContext.addStatement("return null");
        makeContext.endControlFlow();

        makeContext.beginControlFlow("if (comparators[0] != null)");
        makeContext.addComment(
                "the comparator kernel compares in the ascending sense; descending reverses the comparator");
        makeContext.addStatement(
                "final $T comparator = order[0] == $T.Ascending ? comparators[0] : comparators[0].reversed()",
                COMPARATOR, SORTING_ORDER);
        makeContext.addStatement("return $T.createContext(size, new $T[] {comparator})",
                ClassName.get(INDIRECT_PACKAGE, IndirectTimsortKernelFactory.kernelName(
                        new ChunkType[] {ChunkType.Object}, new SortingOrder[] {SortingOrder.Ascending},
                        new boolean[] {true})),
                COMPARATOR);
        makeContext.endControlFlow();

        makeContext.beginControlFlow("switch (chunkTypes[0])");
        for (final ChunkType chunkType : ENGINE_CHUNK_TYPES) {
            makeContext.addCode("case " + chunkType.name() + ":\n");
            makeContext.beginControlFlow("if (order[0] == $T.Ascending)", SORTING_ORDER);
            makeContext.addStatement("return $T.createContext(size)",
                    ClassName.get(INDIRECT_PACKAGE, IndirectTimsortKernelFactory.kernelName(
                            new ChunkType[] {chunkType}, new SortingOrder[] {SortingOrder.Ascending},
                            new boolean[1])));
            makeContext.endControlFlow();
            makeContext.addStatement("return $T.createContext(size)",
                    ClassName.get(INDIRECT_PACKAGE, IndirectTimsortKernelFactory.kernelName(
                            new ChunkType[] {chunkType}, new SortingOrder[] {SortingOrder.Descending},
                            new boolean[1])));
        }
        makeContext.addCode("default: ");
        makeContext.addStatement("return null");
        makeContext.endControlFlow();

        final TypeSpec dispatcher = TypeSpec.classBuilder("IndirectTimsortDispatcher")
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Selects a pregenerated single-column indirect timsort kernel by chunk type, sort "
                        + "direction, and comparator,\nreturning null for multi-column shapes, which "
                        + "IndirectTimsortKernelFactory compiles on demand.\n")
                .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build())
                .addMethod(makeContext.build())
                .build();
        writeFile(INDIRECT_PACKAGE, dispatcher, "IndirectTimsortDispatcher");
    }


    /**
     * The set of kernels to generate; deliberately identical to the set produced by ReplicateSortKernel: descending
     * plain-char permute kernels are omitted (the null-aware kernels are used for engine char sorts), while the
     * values-only char descending kernel does exist. The comparator kernel exists only for ascending long permutation.
     */
    private static List<KernelSpec> allSpecs() {
        final List<KeyKind> standardKinds = List.of(CHAR_KIND, BYTE_KIND, SHORT_KIND, INT_KIND, LONG_KIND,
                FLOAT_KIND, DOUBLE_KIND, NULL_AWARE_CHAR_KIND, OBJECT_KIND);

        final List<KernelSpec> specs = new ArrayList<>();
        for (final ChunkFamily permute : new ChunkFamily[] {null, BYTE_CHUNKS, INT_CHUNKS, LONG_CHUNKS}) {
            for (final KeyKind kind : standardKinds) {
                specs.add(spec(kind, permute, Direction.ASCENDING));
                if (kind == CHAR_KIND && permute != null) {
                    // the descending char permute kernels are not replicated; NullAwareChar serves that purpose
                    continue;
                }
                specs.add(spec(kind, permute, Direction.DESCENDING));
            }
        }
        specs.add(spec(COMPARATOR_KIND, LONG_CHUNKS, Direction.ASCENDING));
        return specs;
    }

    private static KernelSpec spec(final KeyKind kind, final ChunkFamily permute, final Direction direction) {
        return new KernelSpec(Collections.singletonList(new KeyColumn(kind, direction)), permute, direction);
    }

    private static void generate(final KernelSpec spec) throws IOException {
        writeFile(spec.packageName(), new KernelEmitter(spec).emit(), spec.className());
    }

    private static void writeFile(final String packageName, final TypeSpec type, final String className)
            throws IOException {
        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, type).indent("    ");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY\n");
        fileBuilder.addFileComment(
                "****** Run GenerateTimsortKernels or ./gradlew generateTimsortKernels to regenerate\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("@formatter:off");
        System.out.println("Generating " + className + " to " + SOURCE_ROOT);
        fileBuilder.build().writeTo(SOURCE_ROOT);
    }

    /**
     * Emits one kernel class. The permute-payload kernels use instance methods on the outer class (so that the
     * comparator kernel may hold instance comparison state) with a non-static context inner class; the values-only
     * kernels are fully static.
     */
    static final class KernelEmitter {
        private final KernelSpec spec;
        private final KeyColumn key;
        private final ChunkFamily keyChunks;
        private final boolean hasPermute;
        private final boolean instanceKernel;

        private final TypeVariableName sortAttr;
        private final TypeVariableName permuteAttr;
        private final ClassName kernelClass;
        private final ClassName contextClass;
        private final TypeName parameterizedContext;

        KernelEmitter(final KernelSpec spec) {
            this.spec = spec;
            this.key = spec.singleKey();
            this.keyChunks = key.kind.chunks;
            this.hasPermute = spec.permute != null;
            // the permute kernels dispatch through instance methods so comparator state is available; the values-only
            // kernels have no comparator variant and are entirely static
            this.instanceKernel = hasPermute;

            this.sortAttr = TypeVariableName.get(hasPermute ? "SORT_VALUES_ATTR" : "ATTR", ANY);
            this.permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);
            this.kernelClass = ClassName.get(PACKAGE, spec.className());
            this.contextClass = kernelClass.nestedClass(spec.contextName());
            this.parameterizedContext = hasPermute
                    ? ParameterizedTypeName.get(contextClass, sortAttr, permuteAttr)
                    : ParameterizedTypeName.get(contextClass, sortAttr);
        }

        private List<TypeVariableName> attrVariables() {
            return hasPermute ? List.of(sortAttr, permuteAttr) : List.of(sortAttr);
        }

        private TypeName writableKeyChunk() {
            return keyChunks.writableChunkOf(sortAttr);
        }

        private TypeName writablePermuteChunk() {
            return spec.permute.writableChunkOf(permuteAttr);
        }

        private TypeName keyType() {
            return keyChunks.elementType;
        }

        TypeSpec emit() {
            final TypeSpec.Builder builder = TypeSpec.classBuilder(kernelClass)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addJavadoc("This implements a timsort kernel for $L.\n"
                            + "<p>\n"
                            + "<a href=\"https://bugs.python.org/file4451/timsort.txt\">bugs.python.org</a> and\n"
                            + "<a href=\"https://en.wikipedia.org/wiki/Timsort\">Wikipedia</a> do a decent job of "
                            + "describing the algorithm.\n", keyChunks.plural);

            addConstructor(builder);
            builder.addType(emitContext());
            if (instanceKernel) {
                builder.addMethod(emitCreateContextInstance());
            }
            builder.addMethod(emitCreateContextStatic());
            if (hasPermute) {
                builder.addMethod(emitStaticSortWithRuns());
                builder.addMethod(emitStaticSort());
            } else {
                builder.addMethod(emitStaticSortNoPermute());
            }
            builder.addMethod(emitTimSort());
            builder.addMethod(emitDoComparison());
            for (final MethodSpec compareOp : emitCompareOps()) {
                builder.addMethod(compareOp);
            }
            builder.addMethod(emitEnsureMergeInvariants());
            builder.addMethod(emitMerge());
            builder.addMethod(emitFrontMerge());
            builder.addMethod(emitBackMerge());
            builder.addMethod(emitCopyToTemporary());
            builder.addMethod(emitCopyToChunk());
            builder.addMethod(emitUpperBound());
            builder.addMethod(emitLowerBound());
            builder.addMethod(emitBound());
            builder.addMethod(emitInsertionSort());
            builder.addMethod(emitSwap());
            return builder.build();
        }

        private void addConstructor(final TypeSpec.Builder builder) {
            if (key.kind.comparison.requiresInstanceState()) {
                builder.addField(FieldSpec.builder(COMPARATOR, "comparator", Modifier.PRIVATE, Modifier.FINAL)
                        .build());
                builder.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(ParameterSpec.builder(COMPARATOR, "comparator", Modifier.FINAL).build())
                        .addStatement("this.comparator = comparator")
                        .build());
            } else if (instanceKernel) {
                builder.addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build());
            } else {
                builder.addMethod(MethodSpec.constructorBuilder()
                        .addModifiers(Modifier.PRIVATE)
                        .addStatement("throw new $T()", UnsupportedOperationException.class)
                        .build());
            }
        }

        private TypeSpec emitContext() {
            final TypeSpec.Builder context = TypeSpec.classBuilder(spec.contextName())
                    .addModifiers(Modifier.PUBLIC)
                    .addTypeVariables(attrVariables());
            if (!instanceKernel) {
                context.addModifiers(Modifier.STATIC);
            }
            if (hasPermute) {
                context.addSuperinterface(
                        ParameterizedTypeName.get(sortKernelInterface(spec.permute), sortAttr, permuteAttr));
            } else {
                context.addSuperinterface(ENGINE_CONTEXT);
            }

            context.addField(int.class, "minGallop");
            context.addField(FieldSpec.builder(int.class, "runCount").initializer("0").build());
            context.addField(int[].class, "runStarts", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(int[].class, "runLengths", Modifier.PRIVATE, Modifier.FINAL);
            if (hasPermute) {
                context.addField(
                        FieldSpec.builder(writablePermuteChunk(), "temporaryKeys", Modifier.PRIVATE, Modifier.FINAL)
                                .build());
            }
            context.addField(FieldSpec.builder(writableKeyChunk(), "temporaryValues", Modifier.PRIVATE, Modifier.FINAL)
                    .build());

            final MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(int.class, "size");
            if (hasPermute) {
                constructor.addStatement("temporaryKeys = $T.makeWritableChunk((size + 2) / 2)",
                        spec.permute.writableChunkName);
            }
            constructor.addStatement("temporaryValues = $T.makeWritableChunk((size + 2) / 2)",
                    keyChunks.writableChunkName);
            constructor.addStatement("runStarts = new int[(size + 31) / 32]");
            constructor.addStatement("runLengths = new int[(size + 31) / 32]");
            constructor.addStatement("minGallop = $T.INITIAL_GALLOP", TIMSORT_UTILS);
            context.addMethod(constructor.build());

            if (hasPermute) {
                context.addMethod(MethodSpec.methodBuilder("sort")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(writablePermuteChunkParam())
                        .addParameter(ParameterizedTypeName.get(WRITABLE_CHUNK, sortAttr), "valuesToSort")
                        .addStatement("$T.this.sort(this, valuesToPermute, valuesToSort.$L())", kernelClass,
                                keyChunks.asWritableChunkMethod())
                        .build());
                context.addMethod(MethodSpec.methodBuilder("sort")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(writablePermuteChunkParam())
                        .addParameter(ParameterizedTypeName.get(WRITABLE_CHUNK, sortAttr), "valuesToSort")
                        .addParameter(offsetsParam())
                        .addParameter(lengthsParam())
                        .addStatement("$T.this.sort(this, valuesToPermute, valuesToSort.$L(), offsetsIn, lengthsIn)",
                                kernelClass, keyChunks.asWritableChunkMethod())
                        .build());
                context.addMethod(MethodSpec.methodBuilder("sort")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(writablePermuteChunkParam())
                        .addParameter(ParameterizedTypeName.get(WRITABLE_CHUNK, sortAttr), "valuesToSort")
                        .addParameter(int.class, "offset")
                        .addParameter(int.class, "length")
                        .addStatement("$T.this.timSort(this, valuesToPermute, valuesToSort.$L(), offset, length)",
                                kernelClass, keyChunks.asWritableChunkMethod())
                        .build());
                context.addMethod(MethodSpec.methodBuilder("merge")
                        .addAnnotation(Override.class)
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(writablePermuteChunkParam())
                        .addParameter(ParameterizedTypeName.get(WRITABLE_CHUNK, sortAttr), "valuesToSort")
                        .addParameter(int.class, "start1")
                        .addParameter(int.class, "length1")
                        .addParameter(int.class, "length2")
                        .addStatement(
                                "$T.this.merge(this, valuesToPermute, valuesToSort.$L(), start1, length1, length2)",
                                kernelClass, keyChunks.asWritableChunkMethod())
                        .build());
            } else {
                context.addMethod(MethodSpec.methodBuilder("sort")
                        .addModifiers(Modifier.PUBLIC)
                        .addParameter(ParameterizedTypeName.get(WRITABLE_CHUNK, sortAttr), "valuesToSort")
                        .addStatement("$T.sort(this, valuesToSort.$L())", kernelClass,
                                keyChunks.asWritableChunkMethod())
                        .build());
            }

            final MethodSpec.Builder close = MethodSpec.methodBuilder("close")
                    .addModifiers(Modifier.PUBLIC);
            if (hasPermute) {
                close.addAnnotation(Override.class);
                close.addStatement("temporaryKeys.close()");
            }
            close.addStatement("temporaryValues.close()");
            context.addMethod(close.build());

            if (instanceKernel) {
                context.addMethod(MethodSpec.methodBuilder("kernel")
                        .addModifiers(Modifier.PRIVATE)
                        .returns(kernelClass)
                        .addStatement("return $T.this", kernelClass)
                        .build());
            }
            return context.build();
        }

        private ParameterSpec writablePermuteChunkParam() {
            return ParameterSpec.builder(writablePermuteChunk(), "valuesToPermute").build();
        }

        private ParameterSpec offsetsParam() {
            return ParameterSpec
                    .builder(ParameterizedTypeName.get(INT_CHUNK, WildcardTypeName.subtypeOf(CHUNK_POSITIONS)),
                            "offsetsIn")
                    .build();
        }

        private ParameterSpec lengthsParam() {
            return ParameterSpec
                    .builder(ParameterizedTypeName.get(INT_CHUNK, WildcardTypeName.subtypeOf(CHUNK_LENGTHS)),
                            "lengthsIn")
                    .build();
        }

        private MethodSpec emitCreateContextInstance() {
            return MethodSpec.methodBuilder("createContextInstance")
                    .addModifiers(Modifier.PUBLIC)
                    .addTypeVariables(attrVariables())
                    .returns(parameterizedContext)
                    .addParameter(int.class, "size")
                    .addStatement("return new $T<>(size)", contextClass)
                    .build();
        }

        private MethodSpec emitCreateContextStatic() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("createContext")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariables(attrVariables())
                    .returns(parameterizedContext)
                    .addParameter(ParameterSpec.builder(int.class, "size", Modifier.FINAL).build());
            if (key.kind.comparison.requiresInstanceState()) {
                builder.addParameter(ParameterSpec.builder(COMPARATOR, "comparator").build())
                        .addStatement("return new $T(comparator).createContextInstance(size)", kernelClass);
            } else if (instanceKernel) {
                builder.addStatement("return new $T().createContextInstance(size)", kernelClass);
            } else {
                builder.addStatement("return new $T<>(size)", contextClass);
            }
            return builder.build();
        }

        private static final String SORT_WITH_RUNS_JAVADOC =
                "Sort the values in valuesToSort permuting the valuesToPermute chunk in the same way.\n"
                        + "<p>\n"
                        + "The offsetsIn chunk is contains the offset of runs to sort in valuesToPermute; and the "
                        + "lengthsIn contains the\n"
                        + "length of the runs. This allows the kernel to be used for a secondary column sort, chaining "
                        + "it together with\n"
                        + "fewer runs sorted on each pass.\n";

        private MethodSpec emitStaticSortWithRuns() {
            return MethodSpec.methodBuilder("sort")
                    .addModifiers(Modifier.STATIC)
                    .addJavadoc(SORT_WITH_RUNS_JAVADOC)
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context")
                    .addParameter(writablePermuteChunkParam())
                    .addParameter(ParameterSpec.builder(writableKeyChunk(), "valuesToSort").build())
                    .addParameter(offsetsParam())
                    .addParameter(lengthsParam())
                    .addStatement("final int numberRuns = offsetsIn.size()")
                    .beginControlFlow("for (int run = 0; run < numberRuns; ++run)")
                    .addStatement("final int offset = offsetsIn.get(run)")
                    .addStatement("final int length = lengthsIn.get(run)")
                    .addStatement("context.kernel().timSort(context, valuesToPermute, valuesToSort, offset, length)")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitStaticSort() {
            return MethodSpec.methodBuilder("sort")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addJavadoc(SORT_WITH_RUNS_JAVADOC)
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context")
                    .addParameter(writablePermuteChunkParam())
                    .addParameter(ParameterSpec.builder(writableKeyChunk(), "valuesToSort").build())
                    .addStatement("context.kernel().timSort(context, valuesToPermute, valuesToSort, 0, "
                            + "valuesToPermute.size())")
                    .build();
        }

        private MethodSpec emitStaticSortNoPermute() {
            return MethodSpec.methodBuilder("sort")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addJavadoc("Sort the values in valuesToSort.\n")
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context")
                    .addParameter(ParameterSpec.builder(writableKeyChunk(), "valuesToSort", Modifier.FINAL).build())
                    .addStatement("timSort(context, valuesToSort, 0, valuesToSort.size())")
                    .build();
        }

        /** Adds the standard kernel-method parameters: context, permute chunk (if present), values chunk. */
        private MethodSpec.Builder kernelMethod(final String name) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE)
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context");
            if (!instanceKernel) {
                builder.addModifiers(Modifier.STATIC);
            }
            if (hasPermute) {
                builder.addParameter(writablePermuteChunkParam());
            }
            builder.addParameter(ParameterSpec.builder(writableKeyChunk(), "valuesToSort").build());
            return builder;
        }

        private MethodSpec emitTimSort() {
            final String permute = hasPermute ? "valuesToPermute, " : "";
            return kernelMethod("timSort")
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .beginControlFlow("if (length <= 1)")
                    .addStatement("return")
                    .endControlFlow()
                    .addStatement("final int minRun = $T.getRunLength(length)", TIMSORT_UTILS)
                    .beginControlFlow("if (length <= minRun)")
                    .addStatement("insertionSort(" + permute + "valuesToSort, offset, length)")
                    .addStatement("return")
                    .endControlFlow()
                    .addStatement("context.runCount = 0")
                    .addStatement("int startRun = offset")
                    .beginControlFlow("while (startRun < offset + length)")
                    .addStatement("$T current = valuesToSort.get(startRun)", keyType())
                    .addComment("note that endrun is exclusive")
                    .addStatement("int endRun")
                    .addStatement("final boolean descending")
                    .beginControlFlow("if (startRun + 1 == offset + length)")
                    .addStatement("endRun = offset + length")
                    .addStatement("descending = false")
                    .nextControlFlow("else")
                    .addStatement("$T next = valuesToSort.get(startRun + 1)", keyType())
                    .addStatement("endRun = startRun + 2")
                    .addStatement("descending = gt(current, next)")
                    .beginControlFlow("if (!descending)")
                    .addComment("search for a non-descending run")
                    .addStatement("current = next")
                    .beginControlFlow("while (endRun < length && geq(next = valuesToSort.get(endRun), current))")
                    .addStatement("current = next")
                    .addStatement("endRun++")
                    .endControlFlow()
                    .nextControlFlow("else")
                    .addComment(
                            "search for a strictly descending run; we can not have any equal values, or we will break the")
                    .addComment("sort's stability guarantee")
                    .addStatement("current = next")
                    .beginControlFlow("while (endRun < length && lt(next = valuesToSort.get(endRun), current))")
                    .addStatement("current = next")
                    .addStatement("endRun++")
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow()
                    .addStatement("final int foundLength = endRun - startRun")
                    .addStatement("context.runStarts[context.runCount] = startRun")
                    .beginControlFlow("if (foundLength < minRun)")
                    .addComment("increase the size of the run to the minimum run")
                    .addStatement("final int actualLength = Math.min(minRun, length - (startRun - offset))")
                    .addStatement("insertionSort(" + permute + "valuesToSort, startRun, actualLength)")
                    .addStatement("context.runLengths[context.runCount] = actualLength")
                    .addStatement("startRun += actualLength")
                    .nextControlFlow("else")
                    .beginControlFlow("if (descending)")
                    .addComment("reverse the current run")
                    .beginControlFlow("for (int ii = 0; ii < foundLength / 2; ++ii)")
                    .addStatement("swap(" + permute + "valuesToSort, ii + startRun, endRun - ii - 1)")
                    .endControlFlow()
                    .endControlFlow()
                    .addComment("now an ascending run")
                    .addStatement("context.runLengths[context.runCount] = foundLength")
                    .addStatement("startRun = endRun")
                    .endControlFlow()
                    .addStatement("context.runCount++")
                    .addComment("check the invariants at the top of the stack")
                    .addStatement("ensureMergeInvariants(context, " + permute + "valuesToSort)")
                    .endControlFlow()
                    .beginControlFlow("while (context.runCount > 1)")
                    .addStatement("final int length2 = context.runLengths[context.runCount - 1]")
                    .addStatement("final int start1 = context.runStarts[context.runCount - 2]")
                    .addStatement("final int length1 = context.runLengths[context.runCount - 2]")
                    .addStatement("merge(context, " + permute + "valuesToSort, start1, length1, length2)")
                    .addStatement("context.runStarts[context.runCount - 2] = start1")
                    .addStatement("context.runLengths[context.runCount - 2] = length1 + length2")
                    .addStatement("context.runCount--")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitDoComparison() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("doComparison")
                    .addModifiers(Modifier.PRIVATE)
                    .returns(int.class)
                    .addParameter(keyType(), "lhs")
                    .addParameter(keyType(), "rhs");
            if (!key.kind.comparison.requiresInstanceState()) {
                builder.addModifiers(Modifier.STATIC);
            }
            if (spec.direction == Direction.DESCENDING) {
                builder.addComment(
                        "note that this is a descending kernel, thus the comparisons here are backwards (e.g., the lt function is in terms of the sort direction, so is implemented by gt)");
            }
            builder.addStatement("return $L", key.kind.comparison.comparisonExpression(spec.direction));
            return builder.build();
        }

        private List<MethodSpec> emitCompareOps() {
            final List<MethodSpec> ops = new ArrayList<>();
            for (final String[] op : new String[][] {
                    {"gt", ">"}, {"lt", "<"}, {"geq", ">="}, {"leq", "<="}}) {
                final MethodSpec.Builder builder = MethodSpec.methodBuilder(op[0])
                        .addAnnotation(VISIBLE_FOR_TESTING)
                        .returns(boolean.class)
                        .addParameter(keyType(), "lhs")
                        .addParameter(keyType(), "rhs")
                        .addStatement("return doComparison(lhs, rhs) $L 0", op[1]);
                if (!key.kind.comparison.requiresInstanceState()) {
                    builder.addModifiers(Modifier.STATIC);
                }
                ops.add(builder.build());
            }
            return ops;
        }

        private MethodSpec emitEnsureMergeInvariants() {
            final String permute = hasPermute ? "valuesToPermute, " : "";
            return kernelMethod("ensureMergeInvariants")
                    .addJavadoc("<p>\n"
                            + "There are two merge invariants that we must preserve, quoting from Wikipedia:\n"
                            + "<p>\n"
                            + "Timsort is a stable sorting algorithm (order of elements with same key is kept) and "
                            + "strives to perform balanced\n"
                            + "merges (a merge thus merges runs of similar sizes).\n"
                            + "<p>\n"
                            + "In order to achieve sorting stability, only consecutive runs are merged. Between two "
                            + "non-consecutive runs, there\n"
                            + "can be an element with the same key inside the runs. Merging those two runs would "
                            + "change the order of equal keys.\n"
                            + "Example of this situation ([] are ordered runs): [1 2 2] 1 4 2 [0 1 2]\n"
                            + "<p>\n"
                            + "In pursuit of balanced merges, Timsort considers three runs on the top of the stack, "
                            + "X, Y, Z, and maintains the\n"
                            + "invariants:\n"
                            + "<ul>\n"
                            + "<li>|Z| &gt; |Y| + |X|</li>\n"
                            + "<li>|Y| &gt; |X|</li>\n"
                            + "</ul>\n"
                            + "<p>\n"
                            + "If any of these invariants is violated, Y is merged with the smaller of X or Z and the "
                            + "invariants are checked\n"
                            + "again. Once the invariants hold, the search for a new run in the data can start. "
                            + "These invariants maintain merges\n"
                            + "as being approximately balanced while maintaining a compromise between delaying merging "
                            + "for balance, exploiting\n"
                            + "fresh occurrence of runs in cache memory and making merge decisions relatively simple.\n")
                    .beginControlFlow("while (context.runCount > 1)")
                    .addStatement("final int xIndex = context.runCount - 1")
                    .addStatement("final int yIndex = context.runCount - 2")
                    .addStatement("final int zIndex = context.runCount - 3")
                    .addStatement("final int xLen = context.runLengths[xIndex]")
                    .addStatement("final int yLen = context.runLengths[yIndex]")
                    .addStatement("final int zLen = zIndex >= 0 ? context.runLengths[zIndex] : -1")
                    .addStatement("final boolean xMerge")
                    .beginControlFlow("if (zLen >= 0 && (zLen <= yLen + xLen))")
                    .addComment("we must merge the smaller of the two")
                    .addStatement("xMerge = xLen < zLen")
                    .nextControlFlow("else if (yLen < xLen)")
                    .addComment("we must merge Y into X")
                    .addStatement("xMerge = true")
                    .nextControlFlow("else")
                    .addStatement("break")
                    .endControlFlow()
                    .addStatement("final int yStart = context.runStarts[yIndex]")
                    .addStatement("final int xStart = context.runStarts[xIndex]")
                    .beginControlFlow("if (xMerge)")
                    .addComment("merge y and x")
                    .addStatement("merge(context, " + permute + "valuesToSort, yStart, yLen, xLen)")
                    .addComment("unchanged: context.runStarts[yStart];")
                    .addStatement("context.runLengths[yIndex] += xLen")
                    .nextControlFlow("else")
                    .addComment("merge y and z")
                    .addStatement("final int zStart = context.runStarts[zIndex]")
                    .addStatement("merge(context, " + permute + "valuesToSort, zStart, zLen, yLen)")
                    .addComment("unchanged: context.runStarts[zIndex];")
                    .addStatement("context.runLengths[zIndex] += yLen")
                    .addStatement("context.runStarts[yIndex] = xStart")
                    .addStatement("context.runLengths[yIndex] = xLen")
                    .endControlFlow()
                    .addStatement("context.runCount--")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitMerge() {
            final String permute = hasPermute ? "valuesToPermute, " : "";
            return kernelMethod("merge")
                    .addParameter(int.class, "start1")
                    .addParameter(int.class, "length1")
                    .addParameter(int.class, "length2")
                    .addComment(
                            "we know that we can never have zero length runs, because there is a minimum run size enforced; and at the")
                    .addComment(
                            "end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus")
                    .addComment("they'll never be empty. I'm being cheap about function calls and control flow here.")
                    .addComment("Assert.gtZero(length1, \"length1\");")
                    .addComment("Assert.gtZero(length2, \"length2\");")
                    .addStatement("final int start2 = start1 + length1")
                    .addComment("find the location of run2[0] in run1")
                    .addStatement("final $T run2lo = valuesToSort.get(start2)", keyType())
                    .addStatement(
                            "final int mergeStartPosition = upperBound(valuesToSort, start1, start1 + length1, run2lo)")
                    .beginControlFlow("if (mergeStartPosition == start1 + length1)")
                    .addComment("these two runs are sorted already")
                    .addStatement("return")
                    .endControlFlow()
                    .addComment("find the location of run1[length1 - 1] in run2")
                    .addStatement("final $T run1hi = valuesToSort.get(start1 + length1 - 1)", keyType())
                    .addStatement(
                            "final int mergeEndPosition = lowerBound(valuesToSort, start2, start2 + length2, run1hi)")
                    .addComment("figure out which of the two runs is now shorter")
                    .addStatement("final int remaining1 = start1 + length1 - mergeStartPosition")
                    .addStatement("final int remaining2 = mergeEndPosition - start2")
                    .beginControlFlow("if (remaining1 < remaining2)")
                    .addStatement(
                            "copyToTemporary(context, " + permute + "valuesToSort, mergeStartPosition, remaining1)")
                    .addComment(
                            "now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,")
                    .addComment("because we've preserved all the values of run1")
                    .addStatement(
                            "frontMerge(context, " + permute + "valuesToSort, mergeStartPosition, start2, remaining2)")
                    .nextControlFlow("else")
                    .addStatement("copyToTemporary(context, " + permute + "valuesToSort, start2, remaining2)")
                    .addComment(
                            "now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the")
                    .addComment("back, because we've preserved all the values of run2)")
                    .addStatement("backMerge(context, " + permute + "valuesToSort, mergeStartPosition, remaining1)")
                    .endControlFlow()
                    .build();
        }

        private static final String MERGE_JAVADOC_FRONT =
                "Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run "
                        + "length, but\nthe length of things we might need to merge.\n"
                        + "<p>\nWe eventually need to do galloping here, but are skipping that for now\n";

        private MethodSpec emitFrontMerge() {
            final String copyChunkArgs1 = hasPermute
                    ? "context.temporaryKeys, context.temporaryValues, valuesToPermute, valuesToSort"
                    : "context.temporaryValues, valuesToSort";
            final String copyChunkArgs2 = hasPermute
                    ? "valuesToPermute, valuesToSort, valuesToPermute, valuesToSort"
                    : "valuesToSort, valuesToSort";
            final MethodSpec.Builder builder = kernelMethod("frontMerge")
                    .addJavadoc(MERGE_JAVADOC_FRONT)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "start2", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length2", Modifier.FINAL).build())
                    .addStatement("int tempCursor = 0")
                    .addStatement("int run2Cursor = start2")
                    .addStatement("final int run1size = context.temporaryValues.size()")
                    .addStatement("int ii")
                    .addStatement("final int mergeEndExclusive = start2 + length2")
                    .addStatement("$T val1 = context.temporaryValues.get(tempCursor)", keyType())
                    .addStatement("$T val2 = valuesToSort.get(run2Cursor)", keyType())
                    .addStatement("ii = mergeStartPosition")
                    .beginControlFlow("nodataleft: while (ii < mergeEndExclusive)")
                    .addStatement("int run1wins = 0")
                    .addStatement("int run2wins = 0")
                    .beginControlFlow("if (context.minGallop < 2)")
                    .addStatement("context.minGallop = 2")
                    .endControlFlow()
                    .beginControlFlow("while (run1wins < context.minGallop && run2wins < context.minGallop)")
                    .beginControlFlow("if (leq(val1, val2))");
            if (hasPermute) {
                builder.addStatement("valuesToSort.set(ii, val1)")
                        .addStatement("valuesToPermute.set(ii++, context.temporaryKeys.get(tempCursor))");
            } else {
                builder.addStatement("valuesToSort.set(ii++, val1)");
            }
            builder.beginControlFlow("if (++tempCursor == run1size)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1 = context.temporaryValues.get(tempCursor)")
                    .addStatement("run1wins++")
                    .addStatement("run2wins = 0")
                    .nextControlFlow("else");
            if (hasPermute) {
                builder.addStatement("valuesToSort.set(ii, val2)")
                        .addStatement("valuesToPermute.set(ii++, valuesToPermute.get(run2Cursor))");
            } else {
                builder.addStatement("valuesToSort.set(ii++, val2)");
            }
            builder.beginControlFlow("if (++run2Cursor == mergeEndExclusive)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2 = valuesToSort.get(run2Cursor)")
                    .addStatement("run2wins++")
                    .addStatement("run1wins = 0")
                    .endControlFlow()
                    .endControlFlow()
                    .addComment(
                            "we are in galloping mode now, if we had run out of data then we should have already bailed out to")
                    .addComment("nodataleft")
                    .beginControlFlow("while (ii < mergeEndExclusive)")
                    .addComment(
                            "if we had a lot of things from run1, we take the next thing from run2 then find it in run1")
                    .addStatement(
                            "final int copyUntil1 = upperBound(context.temporaryValues, tempCursor, run1size, val2)")
                    .addStatement("final int gallopLength1 = copyUntil1 - tempCursor")
                    .beginControlFlow("if (gallopLength1 > 0)")
                    .addStatement("copyToChunk(" + copyChunkArgs1 + ", tempCursor, ii, gallopLength1)")
                    .addStatement("tempCursor += gallopLength1")
                    .addStatement("ii += gallopLength1")
                    .beginControlFlow("if (tempCursor == run1size)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1 = context.temporaryValues.get(tempCursor)")
                    .addStatement("context.minGallop--")
                    .endControlFlow()
                    .addComment(
                            "if we had a lot of things from run2, we take the next thing from run1 and then find it in run2")
                    .addStatement(
                            "final int copyUntil2 = lowerBound(valuesToSort, run2Cursor, mergeEndExclusive, val1)")
                    .addStatement("final int gallopLength2 = copyUntil2 - run2Cursor")
                    .beginControlFlow("if (gallopLength2 > 0)")
                    .addStatement("copyToChunk(" + copyChunkArgs2 + ", run2Cursor, ii, gallopLength2)")
                    .addStatement("run2Cursor += gallopLength2")
                    .addStatement("ii += gallopLength2")
                    .beginControlFlow("if (run2Cursor == mergeEndExclusive)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2 = valuesToSort.get(run2Cursor)")
                    .addStatement("context.minGallop--")
                    .endControlFlow()
                    .beginControlFlow("if (gallopLength1 < $T.INITIAL_GALLOP && gallopLength2 < $T.INITIAL_GALLOP)",
                            TIMSORT_UTILS, TIMSORT_UTILS)
                    .addComment("undo the possible subtraction from above")
                    .addStatement("context.minGallop += 2")
                    .addStatement("break")
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow()
                    .beginControlFlow("while (tempCursor < run1size)")
                    .addStatement("valuesToSort.set(ii, context.temporaryValues.get(tempCursor))");
            if (hasPermute) {
                builder.addStatement("valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor))");
            }
            return builder
                    .addStatement("tempCursor++")
                    .addStatement("ii++")
                    .endControlFlow()
                    .build();
        }

        private static final String MERGE_JAVADOC_BACK =
                "Merge context temporary and run1 between mergeStartPosition + length1 + temporary.length\n"
                        + "<p>\nWe eventually need to do galloping here, but are skipping that for now\n";

        private MethodSpec emitBackMerge() {
            final String copyChunkArgs1 = hasPermute
                    ? "context.temporaryKeys, context.temporaryValues, valuesToPermute, valuesToSort"
                    : "context.temporaryValues, valuesToSort";
            final String copyChunkArgs2 = hasPermute
                    ? "valuesToPermute, valuesToSort, valuesToPermute, valuesToSort"
                    : "valuesToSort, valuesToSort";
            final MethodSpec.Builder builder = kernelMethod("backMerge")
                    .addJavadoc(MERGE_JAVADOC_BACK)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length1", Modifier.FINAL).build())
                    .addStatement("final int run1End = mergeStartPosition + length1")
                    .addStatement("int run1Cursor = run1End - 1")
                    .addStatement("int tempCursor = context.temporaryValues.size() - 1")
                    .addStatement("final int mergeLength = context.temporaryValues.size() + length1")
                    .addStatement("int ii")
                    .addStatement("$T val1 = valuesToSort.get(run1Cursor)", keyType())
                    .addStatement("$T val2 = context.temporaryValues.get(tempCursor)", keyType())
                    .addStatement("final int mergeEnd = mergeStartPosition + mergeLength")
                    .addStatement("ii = mergeEnd - 1")
                    .beginControlFlow("nodataleft: while (ii >= mergeStartPosition)")
                    .addStatement("int run1wins = 0")
                    .addStatement("int run2wins = 0")
                    .beginControlFlow("if (context.minGallop < 2)")
                    .addStatement("context.minGallop = 2")
                    .endControlFlow()
                    .beginControlFlow("while (run1wins < context.minGallop && run2wins < context.minGallop)")
                    .beginControlFlow("if (geq(val2, val1))");
            if (hasPermute) {
                builder.addStatement("valuesToSort.set(ii, val2)")
                        .addStatement("valuesToPermute.set(ii--, context.temporaryKeys.get(tempCursor))");
            } else {
                builder.addStatement("valuesToSort.set(ii--, val2)");
            }
            builder.beginControlFlow("if (--tempCursor < 0)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2 = context.temporaryValues.get(tempCursor)")
                    .addStatement("run2wins++")
                    .addStatement("run1wins = 0")
                    .nextControlFlow("else");
            if (hasPermute) {
                builder.addStatement("valuesToSort.set(ii, val1)")
                        .addStatement("valuesToPermute.set(ii--, valuesToPermute.get(run1Cursor))");
            } else {
                builder.addStatement("valuesToSort.set(ii--, val1)");
            }
            builder.beginControlFlow("if (--run1Cursor < mergeStartPosition)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1 = valuesToSort.get(run1Cursor)")
                    .addStatement("run1wins++")
                    .addStatement("run2wins = 0")
                    .endControlFlow()
                    .endControlFlow()
                    .addComment(
                            "we are in galloping mode now, if we had run out of data then we should have already bailed out to")
                    .addComment("nodataleft")
                    .beginControlFlow("while (ii >= mergeStartPosition)")
                    .addComment(
                            "if we had a lot of things from run2, we take the next thing from run1 then find it in run2")
                    .addStatement("final int copyUntil2 = lowerBound(context.temporaryValues, 0, tempCursor, val1) + 1")
                    .addStatement("final int gallopLength2 = tempCursor - copyUntil2 + 1")
                    .beginControlFlow("if (gallopLength2 > 0)")
                    .addStatement("copyToChunk(" + copyChunkArgs1
                            + ", copyUntil2, ii - gallopLength2 + 1, gallopLength2)")
                    .addStatement("tempCursor -= gallopLength2")
                    .addStatement("ii -= gallopLength2")
                    .beginControlFlow("if (tempCursor < 0)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2 = context.temporaryValues.get(tempCursor)")
                    .addStatement("context.minGallop--")
                    .endControlFlow()
                    .addComment(
                            "if we had a lot of things from run1, we take the next thing from run2 and then find it in run1")
                    .addStatement(
                            "final int copyUntil1 = upperBound(valuesToSort, mergeStartPosition, run1Cursor, val2)")
                    .addStatement("final int gallopLength1 = run1Cursor - copyUntil1")
                    .beginControlFlow("if (gallopLength1 > 0)")
                    .addStatement("copyToChunk(" + copyChunkArgs2
                            + ", copyUntil1, ii - gallopLength1, gallopLength1 + 1)")
                    .addStatement("run1Cursor -= gallopLength1")
                    .addStatement("ii -= gallopLength1")
                    .beginControlFlow("if (run1Cursor < mergeStartPosition)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1 = valuesToSort.get(run1Cursor)")
                    .addStatement("context.minGallop--")
                    .endControlFlow()
                    .beginControlFlow("if (gallopLength1 < $T.INITIAL_GALLOP && gallopLength2 < $T.INITIAL_GALLOP)",
                            TIMSORT_UTILS, TIMSORT_UTILS)
                    .addComment("undo the possible subtraction from above")
                    .addStatement("context.minGallop += 2")
                    .addStatement("break")
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow()
                    .beginControlFlow("while (tempCursor >= 0)")
                    .addStatement("valuesToSort.set(ii, context.temporaryValues.get(tempCursor))");
            if (hasPermute) {
                builder.addStatement("valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor))");
            }
            return builder
                    .addStatement("tempCursor--")
                    .addStatement("ii--")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitCopyToTemporary() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("copyToTemporary")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context");
            if (hasPermute) {
                builder.addParameter(writablePermuteChunkParam());
            }
            builder.addParameter(ParameterSpec.builder(writableKeyChunk(), "valuesToSort").build())
                    .addParameter(int.class, "mergeStartPosition")
                    .addParameter(int.class, "remaining1")
                    .addStatement("context.temporaryValues.setSize(remaining1)");
            if (hasPermute) {
                builder.addStatement("context.temporaryKeys.setSize(remaining1)");
            }
            builder.addStatement("context.temporaryValues.copyFromChunk(valuesToSort, mergeStartPosition, 0, "
                    + "remaining1)");
            if (hasPermute) {
                builder.addStatement("context.temporaryKeys.copyFromChunk(valuesToPermute, mergeStartPosition, 0, "
                        + "remaining1)");
            }
            return builder.build();
        }

        private MethodSpec emitCopyToChunk() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("copyToChunk")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addTypeVariables(attrVariables());
            if (hasPermute) {
                builder.addParameter(ParameterSpec
                        .builder(spec.permute.chunkOf(permuteAttr), "rowSetSource").build());
                builder.addParameter(ParameterSpec.builder(keyChunks.chunkOf(sortAttr), "valuesSource").build());
                builder.addParameter(ParameterSpec.builder(writablePermuteChunk(), "permuteValuesDest").build());
                builder.addParameter(ParameterSpec.builder(writableKeyChunk(), "sortValuesDest").build());
            } else {
                builder.addParameter(ParameterSpec.builder(keyChunks.chunkOf(sortAttr), "valuesSource").build());
                builder.addParameter(ParameterSpec.builder(writableKeyChunk(), "valuesDest").build());
            }
            builder.addParameter(int.class, "sourceStart")
                    .addParameter(int.class, "destStart")
                    .addParameter(int.class, "length");
            if (hasPermute) {
                builder.addStatement("sortValuesDest.copyFromChunk(valuesSource, sourceStart, destStart, length)");
                builder.addStatement("permuteValuesDest.copyFromChunk(rowSetSource, sourceStart, destStart, length)");
            } else {
                builder.addStatement("valuesDest.copyFromChunk(valuesSource, sourceStart, destStart, length)");
            }
            return builder.build();
        }

        /** The bound methods take a plain (wildcard-attribute) chunk, as they search temporaries and inputs alike. */
        private MethodSpec.Builder boundMethod(final String name) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE)
                    .returns(int.class);
            if (!instanceKernel) {
                builder.addModifiers(Modifier.STATIC);
            }
            return builder;
        }

        private MethodSpec emitUpperBound() {
            return boundMethod("upperBound")
                    .addParameter(ParameterSpec.builder(keyChunks.chunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "lo")
                    .addParameter(int.class, "hi")
                    .addParameter(keyType(), "searchValue")
                    .addComment(
                            "when we binary search in 1, we must identify a position for search value that is *after* our test values;")
                    .addComment("because the values from run 2 may never be inserted before an equal value from run 1")
                    .addComment("")
                    .addComment("lo is inclusive, hi is exclusive")
                    .addComment("")
                    .addComment(
                            "returns the position of the first element that is > searchValue or hi if there is no such element")
                    .addStatement("return bound(valuesToSort, lo, hi, searchValue, false)")
                    .build();
        }

        private MethodSpec emitLowerBound() {
            return boundMethod("lowerBound")
                    .addParameter(ParameterSpec.builder(keyChunks.chunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "lo")
                    .addParameter(int.class, "hi")
                    .addParameter(keyType(), "searchValue")
                    .addComment(
                            "when we binary search in 2, we must identify a position for search value that is *before* our test values;")
                    .addComment("because the values from run 1 may never be inserted after an equal value from run 2")
                    .addStatement("return bound(valuesToSort, lo, hi, searchValue, true)")
                    .build();
        }

        private MethodSpec emitBound() {
            return boundMethod("bound")
                    .addParameter(ParameterSpec.builder(keyChunks.chunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "lo")
                    .addParameter(int.class, "hi")
                    .addParameter(keyType(), "searchValue")
                    .addParameter(ParameterSpec.builder(boolean.class, "lower", Modifier.FINAL).build())
                    .addComment("lt or leq")
                    .addStatement("final int compareLimit = lower ? -1 : 0")
                    .beginControlFlow("while (lo < hi)")
                    .addStatement("final int mid = (lo + hi) >>> 1")
                    .addStatement("final $T testValue = valuesToSort.get(mid)", keyType())
                    .addStatement("final boolean moveLo = doComparison(testValue, searchValue) <= compareLimit")
                    .beginControlFlow("if (moveLo)")
                    .addComment(
                            "For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]")
                    .addStatement("lo = mid + 1")
                    .nextControlFlow("else")
                    .addStatement("hi = mid")
                    .endControlFlow()
                    .endControlFlow()
                    .addStatement("return lo")
                    .build();
        }

        private MethodSpec emitInsertionSort() {
            final String permute = hasPermute ? "valuesToPermute, " : "";
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("insertionSort")
                    .addModifiers(Modifier.PRIVATE);
            if (!instanceKernel) {
                builder.addModifiers(Modifier.STATIC);
            }
            if (hasPermute) {
                builder.addParameter(
                        ParameterSpec.builder(spec.permute.writableChunkOfWildcard(), "valuesToPermute").build());
            }
            return builder
                    .addParameter(ParameterSpec.builder(keyChunks.writableChunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addComment(
                            "this could eventually be done with intrinsics (AVX 512/64 bits for long keys == 16 elements, and can be")
                    .addComment("combined up to 256)")
                    .beginControlFlow("for (int ii = offset + 1; ii < offset + length; ++ii)")
                    .beginControlFlow(
                            "for (int jj = ii; jj > offset && gt(valuesToSort.get(jj - 1), valuesToSort.get(jj)); jj--)")
                    .addStatement("swap(" + permute + "valuesToSort, jj, jj - 1)")
                    .endControlFlow()
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitSwap() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("swap")
                    .addModifiers(Modifier.STATIC, Modifier.PRIVATE);
            if (hasPermute) {
                builder.addParameter(
                        ParameterSpec.builder(spec.permute.writableChunkOfWildcard(), "valuesToPermute").build());
            }
            builder.addParameter(ParameterSpec.builder(keyChunks.writableChunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "a")
                    .addParameter(int.class, "b");
            final String tempName = "temp" + keyChunks.name;
            if (hasPermute) {
                builder.addStatement("final $T tempPermuteValue = valuesToPermute.get(a)", spec.permute.elementType)
                        .addStatement("final $T $L = valuesToSort.get(a)", keyType(), tempName)
                        .addStatement("valuesToPermute.set(a, valuesToPermute.get(b))")
                        .addStatement("valuesToSort.set(a, valuesToSort.get(b))")
                        .addStatement("valuesToPermute.set(b, tempPermuteValue)")
                        .addStatement("valuesToSort.set(b, $L)", tempName);
            } else {
                builder.addStatement("final $T $L = valuesToSort.get(a)", keyType(), tempName)
                        .addStatement("valuesToSort.set(a, valuesToSort.get(b))")
                        .addStatement("valuesToSort.set(b, $L)", tempName);
            }
            return builder.build();
        }
    }



}
