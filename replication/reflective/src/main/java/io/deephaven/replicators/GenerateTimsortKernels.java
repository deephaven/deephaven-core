//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.palantir.javapoet.AnnotationSpec;
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

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String PACKAGE = "io.deephaven.engine.table.impl.sort.timsort2";
    private static final String MULTI_PACKAGE = PACKAGE + ".multi";
    private static final File SOURCE_ROOT = new File("engine/table/src/main/java/");

    private static final ClassName MULTI_COLUMN_SORT_KERNEL =
            ClassName.get("io.deephaven.engine.table.impl.sort", "MultiColumnSortKernel");
    private static final ClassName CHUNK_TYPE = ClassName.get("io.deephaven.chunk", "ChunkType");
    private static final ClassName SORTING_ORDER = ClassName.get("io.deephaven.engine.table.impl", "SortingOrder");

    private static final ClassName ANY = ClassName.get("io.deephaven.chunk.attributes", "Any");
    private static final ClassName CHUNK_POSITIONS = ClassName.get("io.deephaven.chunk.attributes", "ChunkPositions");
    private static final ClassName CHUNK_LENGTHS = ClassName.get("io.deephaven.chunk.attributes", "ChunkLengths");
    private static final ClassName INT_CHUNK = ClassName.get("io.deephaven.chunk", "IntChunk");
    private static final ClassName WRITABLE_CHUNK = ClassName.get("io.deephaven.chunk", "WritableChunk");
    private static final ClassName ENGINE_CONTEXT = ClassName.get("io.deephaven.engine.table", "Context");
    private static final ClassName TIMSORT_UTILS =
            ClassName.get("io.deephaven.engine.table.impl.sort.timsort", "TimsortUtils");
    private static final ClassName VISIBLE_FOR_TESTING =
            ClassName.get("io.deephaven.util.annotations", "VisibleForTesting");
    private static final ClassName COMPARATOR = ClassName.get("java.util", "Comparator");

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

        ChunkFamily(final String name, final String plural, final TypeName elementType) {
            this.name = name;
            this.plural = plural;
            this.elementType = elementType;
            this.chunkName = ClassName.get("io.deephaven.chunk", name + "Chunk");
            this.writableChunkName = ClassName.get("io.deephaven.chunk", "Writable" + name + "Chunk");
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

    private static final ChunkFamily CHAR_CHUNKS = new ChunkFamily("Char", "Characters", TypeName.CHAR);
    private static final ChunkFamily BYTE_CHUNKS = new ChunkFamily("Byte", "Bytes", TypeName.BYTE);
    private static final ChunkFamily SHORT_CHUNKS = new ChunkFamily("Short", "Shorts", TypeName.SHORT);
    private static final ChunkFamily INT_CHUNKS = new ChunkFamily("Int", "Integers", TypeName.INT);
    private static final ChunkFamily LONG_CHUNKS = new ChunkFamily("Long", "Longs", TypeName.LONG);
    private static final ChunkFamily FLOAT_CHUNKS = new ChunkFamily("Float", "Floats", TypeName.FLOAT);
    private static final ChunkFamily DOUBLE_CHUNKS = new ChunkFamily("Double", "Doubles", TypeName.DOUBLE);
    private static final ChunkFamily OBJECT_CHUNKS = new ChunkFamily("Object", "Objects", ClassName.OBJECT);

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
        private final ClassName compareClass =
                ClassName.get("io.deephaven.util.compare", "ObjectComparisons");

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

    private static KeyKind boxedKind(final ChunkFamily chunks, final Class<?> boxed) {
        return new KeyKind(chunks.name, chunks, new StaticCompare(ClassName.get(boxed)));
    }

    private static KeyKind comparisonsKind(final String namePart, final ChunkFamily chunks) {
        return new KeyKind(namePart, chunks,
                new StaticCompare(ClassName.get("io.deephaven.util.compare", chunks.name + "Comparisons")));
    }

    private static final KeyKind CHAR_KIND = boxedKind(CHAR_CHUNKS, Character.class);
    private static final KeyKind BYTE_KIND = boxedKind(BYTE_CHUNKS, Byte.class);
    private static final KeyKind SHORT_KIND = boxedKind(SHORT_CHUNKS, Short.class);
    private static final KeyKind INT_KIND = boxedKind(INT_CHUNKS, Integer.class);
    private static final KeyKind LONG_KIND = boxedKind(LONG_CHUNKS, Long.class);
    private static final KeyKind FLOAT_KIND = comparisonsKind("Float", FLOAT_CHUNKS);
    private static final KeyKind DOUBLE_KIND = comparisonsKind("Double", DOUBLE_CHUNKS);
    private static final KeyKind NULL_AWARE_CHAR_KIND =
            new KeyKind("NullAwareChar", CHAR_CHUNKS,
                    new StaticCompare(ClassName.get("io.deephaven.util.compare", "CharComparisons")));
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
        /** Indirect multi-column kernels sort a parallel position chunk rather than moving the column values. */
        final boolean indirect;

        KernelSpec(final List<KeyColumn> keys, final ChunkFamily permute, final Direction direction) {
            this(keys, permute, direction, false);
        }

        KernelSpec(final List<KeyColumn> keys, final ChunkFamily permute, final Direction direction,
                final boolean indirect) {
            this.keys = keys;
            this.permute = permute;
            this.direction = direction;
            this.indirect = indirect;
        }

        KeyColumn singleKey() {
            if (keys.size() != 1) {
                throw new IllegalStateException("single-column emitter used for a multi-column spec");
            }
            return keys.get(0);
        }

        boolean isMultiColumn() {
            return keys.size() > 1;
        }

        String packageName() {
            return (isMultiColumn() || indirect) ? MULTI_PACKAGE : PACKAGE;
        }

        private String keyNameParts() {
            final StringBuilder builder = new StringBuilder();
            for (final KeyColumn key : keys) {
                builder.append(key.kind.namePart);
            }
            return builder.toString();
        }

        String className() {
            if (indirect) {
                // indirect kernels always permute long row keys, so the permute type is not part of the name
                return keyNameParts()
                        + (isMultiColumn() ? "IndirectMultiColumnTimsortKernel" : "IndirectTimsortKernel");
            }
            if (isMultiColumn()) {
                // multi-column kernels always permute long row keys, so the permute type is not part of the name
                return keyNameParts() + "MultiColumnTimsortKernel";
            }
            final StringBuilder builder = new StringBuilder(keyNameParts());
            if (permute != null) {
                builder.append(permute.name);
            }
            builder.append(direction == Direction.DESCENDING ? "TimsortDescendingKernel" : "TimsortKernel");
            return builder.toString();
        }

        String contextName() {
            if (indirect) {
                return keyNameParts()
                        + (isMultiColumn() ? "IndirectMultiColumnSortKernelContext" : "IndirectSortKernelContext");
            }
            if (isMultiColumn()) {
                return keyNameParts() + "MultiColumnSortKernelContext";
            }
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
        for (final KernelSpec spec : multiColumnSpecs(false)) {
            generate(spec);
        }
        for (final KernelSpec spec : multiColumnSpecs(true)) {
            generate(spec);
        }
        // single-column indirect kernels, dispatched for one-column ascending sorts by the indirect dispatcher
        for (final KeyKind kind : ENGINE_KINDS) {
            generate(new KernelSpec(Collections.singletonList(new KeyColumn(kind, Direction.ASCENDING)),
                    LONG_CHUNKS, Direction.ASCENDING, true));
        }
        generateMultiColumnDispatcher(false);
        generateMultiColumnDispatcher(true);
    }

    /**
     * The key kind used for a column of the given chunk type in an engine sort, matching LongSortKernel.makeContext
     * (chars sort with Deephaven null-aware semantics).
     */
    private static final List<KeyKind> ENGINE_KINDS = List.of(NULL_AWARE_CHAR_KIND, BYTE_KIND, SHORT_KIND, INT_KIND,
            LONG_KIND, FLOAT_KIND, DOUBLE_KIND, OBJECT_KIND);

    /**
     * Multi-column kernels: every pair of engine column kinds, ascending, permuting long row keys. Descending and wider
     * keys dispatch back to the single-column pipeline for now.
     */
    private static List<KernelSpec> multiColumnSpecs(final boolean indirect) {
        final List<KernelSpec> specs = new ArrayList<>();
        for (final KeyKind kind0 : ENGINE_KINDS) {
            for (final KeyKind kind1 : ENGINE_KINDS) {
                specs.add(new KernelSpec(
                        List.of(new KeyColumn(kind0, Direction.ASCENDING), new KeyColumn(kind1, Direction.ASCENDING)),
                        LONG_CHUNKS, Direction.ASCENDING, indirect));
            }
        }
        return specs;
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
        final TypeSpec type;
        if (spec.indirect) {
            type = new IndirectMultiColumnKernelEmitter(spec).emit();
        } else if (spec.isMultiColumn()) {
            type = new MultiColumnKernelEmitter(spec).emit();
        } else {
            type = new KernelEmitter(spec).emit();
        }
        writeFile(spec.packageName(), type, spec.className());
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

        private TypeName keyChunk() {
            return keyChunks.chunkOf(sortAttr);
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

        /** Named arguments shared by every method body template. */
        private Map<String, Object> namedArgs() {
            final Map<String, Object> args = new HashMap<>();
            args.put("kernel", kernelClass);
            args.put("context", contextClass);
            args.put("kt", keyType());
            args.put("wkchunk", keyChunks.writableChunkName);
            args.put("utils", TIMSORT_UTILS);
            args.put("asWritable", keyChunks.asWritableChunkMethod());
            args.put("tempName", "temp" + keyChunks.name);
            if (hasPermute) {
                args.put("pt", spec.permute.elementType);
                args.put("wpchunk", spec.permute.writableChunkName);
            }
            return args;
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
                final ClassName sortKernelInterface =
                        ClassName.get("io.deephaven.engine.table.impl.sort", spec.permute.name + "SortKernel");
                context.addSuperinterface(ParameterizedTypeName.get(sortKernelInterface, sortAttr, permuteAttr));
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
                    .addCode(CodeBlock.builder().addNamed(""
                            + "final int numberRuns = offsetsIn.size();\n"
                            + "for (int run = 0; run < numberRuns; ++run) {\n"
                            + "    final int offset = offsetsIn.get(run);\n"
                            + "    final int length = lengthsIn.get(run);\n"
                            + "\n"
                            + "    context.kernel().timSort(context, valuesToPermute, valuesToSort, offset, length);\n"
                            + "}\n", namedArgs()).build())
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

        // The insertion sort / swap calls carry the permute chunk as their leading argument when present.
        private String permutePrefix() {
            return hasPermute ? "valuesToPermute, " : "";
        }

        private MethodSpec emitTimSort() {
            final Map<String, Object> args = namedArgs();
            args.put("permutePrefix", permutePrefix());
            args.put("contextPermutePrefix", hasPermute ? "valuesToPermute, " : "");
            return kernelMethod("timSort")
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addCode(CodeBlock.builder().addNamed(""
                            + "if (length <= 1) {\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "final int minRun = $utils:T.getRunLength(length);\n"
                            + "\n"
                            + "if (length <= minRun) {\n"
                            + "    insertionSort($permutePrefix:LvaluesToSort, offset, length);\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "context.runCount = 0;\n"
                            + "\n"
                            + "int startRun = offset;\n"
                            + "while (startRun < offset + length) {\n"
                            + "    $kt:T current = valuesToSort.get(startRun);\n"
                            + "\n"
                            + "    int endRun; // note that endrun is exclusive\n"
                            + "    final boolean descending;\n"
                            + "\n"
                            + "    if (startRun + 1 == offset + length) {\n"
                            + "        endRun = offset + length;\n"
                            + "        descending = false;\n"
                            + "    } else {\n"
                            + "        $kt:T next = valuesToSort.get(startRun + 1);\n"
                            + "        endRun = startRun + 2;\n"
                            + "        descending = gt(current, next);\n"
                            + "\n"
                            + "        if (!descending) {\n"
                            + "            // search for a non-descending run\n"
                            + "            current = next;\n"
                            + "            while (endRun < length && geq(next = valuesToSort.get(endRun), current)) {\n"
                            + "                current = next;\n"
                            + "                endRun++;\n"
                            + "            }\n"
                            + "        } else {\n"
                            + "            // search for a strictly descending run; we can not have any equal values, or we will break the\n"
                            + "            // sort's stability guarantee\n"
                            + "            current = next;\n"
                            + "            while (endRun < length && lt(next = valuesToSort.get(endRun), current)) {\n"
                            + "                current = next;\n"
                            + "                endRun++;\n"
                            + "            }\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    final int foundLength = endRun - startRun;\n"
                            + "    context.runStarts[context.runCount] = startRun;\n"
                            + "    if (foundLength < minRun) {\n"
                            + "        // increase the size of the run to the minimum run\n"
                            + "        final int actualLength = Math.min(minRun, length - (startRun - offset));\n"
                            + "        insertionSort($permutePrefix:LvaluesToSort, startRun, actualLength);\n"
                            + "        context.runLengths[context.runCount] = actualLength;\n"
                            + "        startRun += actualLength;\n"
                            + "    } else {\n"
                            + "        if (descending) {\n"
                            + "            // reverse the current run\n"
                            + "            for (int ii = 0; ii < foundLength / 2; ++ii) {\n"
                            + "                swap($permutePrefix:LvaluesToSort, ii + startRun, endRun - ii - 1);\n"
                            + "            }\n"
                            + "        }\n"
                            + "        // now an ascending run\n"
                            + "        context.runLengths[context.runCount] = foundLength;\n"
                            + "        startRun = endRun;\n"
                            + "    }\n"
                            + "\n"
                            + "    context.runCount++;\n"
                            + "\n"
                            + "    // check the invariants at the top of the stack\n"
                            + "    ensureMergeInvariants(context, $contextPermutePrefix:LvaluesToSort);\n"
                            + "}\n"
                            + "\n"
                            + "while (context.runCount > 1) {\n"
                            + "    final int length2 = context.runLengths[context.runCount - 1];\n"
                            + "    final int start1 = context.runStarts[context.runCount - 2];\n"
                            + "    final int length1 = context.runLengths[context.runCount - 2];\n"
                            + "    merge(context, $contextPermutePrefix:LvaluesToSort, start1, length1, length2);\n"
                            + "    context.runStarts[context.runCount - 2] = start1;\n"
                            + "    context.runLengths[context.runCount - 2] = length1 + length2;\n"
                            + "    context.runCount--;\n"
                            + "}\n", args).build())
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
            final Map<String, Object> args = namedArgs();
            args.put("contextPermutePrefix", hasPermute ? "valuesToPermute, " : "");
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
                    .addCode(CodeBlock.builder().addNamed(""
                            + "while (context.runCount > 1) {\n"
                            + "    final int xIndex = context.runCount - 1;\n"
                            + "    final int yIndex = context.runCount - 2;\n"
                            + "    final int zIndex = context.runCount - 3;\n"
                            + "\n"
                            + "    final int xLen = context.runLengths[xIndex];\n"
                            + "    final int yLen = context.runLengths[yIndex];\n"
                            + "    final int zLen = zIndex >= 0 ? context.runLengths[zIndex] : -1;\n"
                            + "\n"
                            + "    final boolean xMerge;\n"
                            + "\n"
                            + "    if (zLen >= 0 && (zLen <= yLen + xLen)) {\n"
                            + "        // we must merge the smaller of the two\n"
                            + "        xMerge = xLen < zLen;\n"
                            + "    } else if (yLen < xLen) {\n"
                            + "        // we must merge Y into X\n"
                            + "        xMerge = true;\n"
                            + "    } else {\n"
                            + "        break;\n"
                            + "    }\n"
                            + "\n"
                            + "    final int yStart = context.runStarts[yIndex];\n"
                            + "    final int xStart = context.runStarts[xIndex];\n"
                            + "    if (xMerge) {\n"
                            + "        // merge y and x\n"
                            + "        merge(context, $contextPermutePrefix:LvaluesToSort, yStart, yLen, xLen);\n"
                            + "\n"
                            + "        // unchanged: context.runStarts[yStart];\n"
                            + "        context.runLengths[yIndex] += xLen;\n"
                            + "    } else {\n"
                            + "        // merge y and z\n"
                            + "        final int zStart = context.runStarts[zIndex];\n"
                            + "        merge(context, $contextPermutePrefix:LvaluesToSort, zStart, zLen, yLen);\n"
                            + "\n"
                            + "        // unchanged: context.runStarts[zIndex];\n"
                            + "        context.runLengths[zIndex] += yLen;\n"
                            + "        context.runStarts[yIndex] = xStart;\n"
                            + "        context.runLengths[yIndex] = xLen;\n"
                            + "    }\n"
                            + "    context.runCount--;\n"
                            + "}\n", args).build())
                    .build();
        }

        private MethodSpec emitMerge() {
            final Map<String, Object> args = namedArgs();
            args.put("contextPermutePrefix", hasPermute ? "valuesToPermute, " : "");
            return kernelMethod("merge")
                    .addParameter(int.class, "start1")
                    .addParameter(int.class, "length1")
                    .addParameter(int.class, "length2")
                    .addCode(CodeBlock.builder().addNamed(""
                            + "// we know that we can never have zero length runs, because there is a minimum run size enforced; and at the\n"
                            + "// end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus\n"
                            + "// they'll never be empty. I'm being cheap about function calls and control flow here.\n"
                            + "// Assert.gtZero(length1, \"length1\");\n"
                            + "// Assert.gtZero(length2, \"length2\");\n"
                            + "\n"
                            + "final int start2 = start1 + length1;\n"
                            + "// find the location of run2[0] in run1\n"
                            + "final $kt:T run2lo = valuesToSort.get(start2);\n"
                            + "final int mergeStartPosition = upperBound(valuesToSort, start1, start1 + length1, run2lo);\n"
                            + "\n"
                            + "if (mergeStartPosition == start1 + length1) {\n"
                            + "    // these two runs are sorted already\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "// find the location of run1[length1 - 1] in run2\n"
                            + "final $kt:T run1hi = valuesToSort.get(start1 + length1 - 1);\n"
                            + "final int mergeEndPosition = lowerBound(valuesToSort, start2, start2 + length2, run1hi);\n"
                            + "\n"
                            + "// figure out which of the two runs is now shorter\n"
                            + "final int remaining1 = start1 + length1 - mergeStartPosition;\n"
                            + "final int remaining2 = mergeEndPosition - start2;\n"
                            + "\n"
                            + "if (remaining1 < remaining2) {\n"
                            + "    copyToTemporary(context, $contextPermutePrefix:LvaluesToSort, mergeStartPosition, remaining1);\n"
                            + "    // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,\n"
                            + "    // because we've preserved all the values of run1\n"
                            + "    frontMerge(context, $contextPermutePrefix:LvaluesToSort, mergeStartPosition, start2, remaining2);\n"
                            + "} else {\n"
                            + "    copyToTemporary(context, $contextPermutePrefix:LvaluesToSort, start2, remaining2);\n"
                            + "    // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the\n"
                            + "    // back, because we've preserved all the values of run2)\n"
                            + "    backMerge(context, $contextPermutePrefix:LvaluesToSort, mergeStartPosition, remaining1);\n"
                            + "}\n", args).build())
                    .build();
        }

        private static final String MERGE_JAVADOC_FRONT =
                "Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run "
                        + "length, but\nthe length of things we might need to merge.\n"
                        + "<p>\nWe eventually need to do galloping here, but are skipping that for now\n";

        private MethodSpec emitFrontMerge() {
            final Map<String, Object> args = namedArgs();
            args.put("setPermute", hasPermute
                    ? "\n            valuesToPermute.set(ii++, context.temporaryKeys.get(tempCursor));"
                    : "");
            final String setValues1 = hasPermute
                    ? "valuesToSort.set(ii, val1);$setPermute:L"
                    : "valuesToSort.set(ii++, val1);";
            args.put("setPermute2", hasPermute
                    ? "\n            valuesToPermute.set(ii++, valuesToPermute.get(run2Cursor));"
                    : "");
            final String setValues2 = hasPermute
                    ? "valuesToSort.set(ii, val2);$setPermute2:L"
                    : "valuesToSort.set(ii++, val2);";
            args.put("copyChunkArgs1", hasPermute
                    ? "context.temporaryKeys, context.temporaryValues, valuesToPermute, valuesToSort"
                    : "context.temporaryValues, valuesToSort");
            args.put("copyChunkArgs2", hasPermute
                    ? "valuesToPermute, valuesToSort, valuesToPermute, valuesToSort"
                    : "valuesToSort, valuesToSort");
            args.put("finalCopyPermute", hasPermute
                    ? "\n    valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));"
                    : "");
            return kernelMethod("frontMerge")
                    .addJavadoc(MERGE_JAVADOC_FRONT)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "start2", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length2", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "int tempCursor = 0;\n"
                            + "int run2Cursor = start2;\n"
                            + "\n"
                            + "final int run1size = context.temporaryValues.size();\n"
                            + "int ii;\n"
                            + "final int mergeEndExclusive = start2 + length2;\n"
                            + "\n"
                            + "$kt:T val1 = context.temporaryValues.get(tempCursor);\n"
                            + "$kt:T val2 = valuesToSort.get(run2Cursor);\n"
                            + "\n"
                            + "ii = mergeStartPosition;\n"
                            + "\n"
                            + "nodataleft: while (ii < mergeEndExclusive) {\n"
                            + "    int run1wins = 0;\n"
                            + "    int run2wins = 0;\n"
                            + "\n"
                            + "    if (context.minGallop < 2) {\n"
                            + "        context.minGallop = 2;\n"
                            + "    }\n"
                            + "\n"
                            + "    while (run1wins < context.minGallop && run2wins < context.minGallop) {\n"
                            + "        if (leq(val1, val2)) {\n"
                            + "            " + setValues1 + "\n"
                            + "\n"
                            + "            if (++tempCursor == run1size) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "\n"
                            + "            val1 = context.temporaryValues.get(tempCursor);\n"
                            + "            run1wins++;\n"
                            + "            run2wins = 0;\n"
                            + "        } else {\n"
                            + "            " + setValues2 + "\n"
                            + "\n"
                            + "            if (++run2Cursor == mergeEndExclusive) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2 = valuesToSort.get(run2Cursor);\n"
                            + "\n"
                            + "            run2wins++;\n"
                            + "            run1wins = 0;\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    // we are in galloping mode now, if we had run out of data then we should have already bailed out to\n"
                            + "    // nodataleft\n"
                            + "    while (ii < mergeEndExclusive) {\n"
                            + "        // if we had a lot of things from run1, we take the next thing from run2 then find it in run1\n"
                            + "        final int copyUntil1 = upperBound(context.temporaryValues, tempCursor, run1size, val2);\n"
                            + "        final int gallopLength1 = copyUntil1 - tempCursor;\n"
                            + "        if (gallopLength1 > 0) {\n"
                            + "            copyToChunk($copyChunkArgs1:L, tempCursor, ii, gallopLength1);\n"
                            + "            tempCursor += gallopLength1;\n"
                            + "            ii += gallopLength1;\n"
                            + "\n"
                            + "            if (tempCursor == run1size) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1 = context.temporaryValues.get(tempCursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        // if we had a lot of things from run2, we take the next thing from run1 and then find it in run2\n"
                            + "        final int copyUntil2 = lowerBound(valuesToSort, run2Cursor, mergeEndExclusive, val1);\n"
                            + "        final int gallopLength2 = copyUntil2 - run2Cursor;\n"
                            + "        if (gallopLength2 > 0) {\n"
                            + "            copyToChunk($copyChunkArgs2:L, run2Cursor, ii, gallopLength2);\n"
                            + "            run2Cursor += gallopLength2;\n"
                            + "            ii += gallopLength2;\n"
                            + "\n"
                            + "            if (run2Cursor == mergeEndExclusive) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2 = valuesToSort.get(run2Cursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        if (gallopLength1 < $utils:T.INITIAL_GALLOP && gallopLength2 < $utils:T.INITIAL_GALLOP) {\n"
                            + "            context.minGallop += 2; // undo the possible subtraction from above\n"
                            + "            break;\n"
                            + "        }\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "while (tempCursor < run1size) {\n"
                            + "    valuesToSort.set(ii, context.temporaryValues.get(tempCursor));$finalCopyPermute:L\n"
                            + "    tempCursor++;\n"
                            + "    ii++;\n"
                            + "}\n", args).build())
                    .build();
        }

        private static final String MERGE_JAVADOC_BACK =
                "Merge context temporary and run1 between mergeStartPosition + length1 + temporary.length\n"
                        + "<p>\nWe eventually need to do galloping here, but are skipping that for now\n";

        private MethodSpec emitBackMerge() {
            final Map<String, Object> args = namedArgs();
            args.put("setPermute", hasPermute
                    ? "\n            valuesToPermute.set(ii--, context.temporaryKeys.get(tempCursor));"
                    : "");
            final String setValues2 = hasPermute
                    ? "valuesToSort.set(ii, val2);$setPermute:L"
                    : "valuesToSort.set(ii--, val2);";
            args.put("setPermute1", hasPermute
                    ? "\n            valuesToPermute.set(ii--, valuesToPermute.get(run1Cursor));"
                    : "");
            final String setValues1 = hasPermute
                    ? "valuesToSort.set(ii, val1);$setPermute1:L"
                    : "valuesToSort.set(ii--, val1);";
            args.put("copyChunkArgs1", hasPermute
                    ? "context.temporaryKeys, context.temporaryValues, valuesToPermute, valuesToSort"
                    : "context.temporaryValues, valuesToSort");
            args.put("copyChunkArgs2", hasPermute
                    ? "valuesToPermute, valuesToSort, valuesToPermute, valuesToSort"
                    : "valuesToSort, valuesToSort");
            args.put("finalCopyPermute", hasPermute
                    ? "\n    valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));"
                    : "");
            return kernelMethod("backMerge")
                    .addJavadoc(MERGE_JAVADOC_BACK)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length1", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "final int run1End = mergeStartPosition + length1;\n"
                            + "int run1Cursor = run1End - 1;\n"
                            + "int tempCursor = context.temporaryValues.size() - 1;\n"
                            + "\n"
                            + "final int mergeLength = context.temporaryValues.size() + length1;\n"
                            + "int ii;\n"
                            + "\n"
                            + "$kt:T val1 = valuesToSort.get(run1Cursor);\n"
                            + "$kt:T val2 = context.temporaryValues.get(tempCursor);\n"
                            + "\n"
                            + "final int mergeEnd = mergeStartPosition + mergeLength;\n"
                            + "ii = mergeEnd - 1;\n"
                            + "\n"
                            + "nodataleft: while (ii >= mergeStartPosition) {\n"
                            + "    int run1wins = 0;\n"
                            + "    int run2wins = 0;\n"
                            + "\n"
                            + "    if (context.minGallop < 2) {\n"
                            + "        context.minGallop = 2;\n"
                            + "    }\n"
                            + "\n"
                            + "    while (run1wins < context.minGallop && run2wins < context.minGallop) {\n"
                            + "        if (geq(val2, val1)) {\n"
                            + "            " + setValues2 + "\n"
                            + "\n"
                            + "            if (--tempCursor < 0) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2 = context.temporaryValues.get(tempCursor);\n"
                            + "\n"
                            + "            run2wins++;\n"
                            + "            run1wins = 0;\n"
                            + "        } else {\n"
                            + "            " + setValues1 + "\n"
                            + "\n"
                            + "            if (--run1Cursor < mergeStartPosition) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1 = valuesToSort.get(run1Cursor);\n"
                            + "\n"
                            + "            run1wins++;\n"
                            + "            run2wins = 0;\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    // we are in galloping mode now, if we had run out of data then we should have already bailed out to\n"
                            + "    // nodataleft\n"
                            + "    while (ii >= mergeStartPosition) {\n"
                            + "        // if we had a lot of things from run2, we take the next thing from run1 then find it in run2\n"
                            + "        final int copyUntil2 = lowerBound(context.temporaryValues, 0, tempCursor, val1) + 1;\n"
                            + "\n"
                            + "        final int gallopLength2 = tempCursor - copyUntil2 + 1;\n"
                            + "        if (gallopLength2 > 0) {\n"
                            + "            copyToChunk($copyChunkArgs1:L, copyUntil2, ii - gallopLength2 + 1, gallopLength2);\n"
                            + "            tempCursor -= gallopLength2;\n"
                            + "            ii -= gallopLength2;\n"
                            + "\n"
                            + "            if (tempCursor < 0) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2 = context.temporaryValues.get(tempCursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1\n"
                            + "        final int copyUntil1 = upperBound(valuesToSort, mergeStartPosition, run1Cursor, val2);\n"
                            + "\n"
                            + "        final int gallopLength1 = run1Cursor - copyUntil1;\n"
                            + "        if (gallopLength1 > 0) {\n"
                            + "            copyToChunk($copyChunkArgs2:L, copyUntil1, ii - gallopLength1, gallopLength1 + 1);\n"
                            + "            run1Cursor -= gallopLength1;\n"
                            + "            ii -= gallopLength1;\n"
                            + "\n"
                            + "            if (run1Cursor < mergeStartPosition) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1 = valuesToSort.get(run1Cursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        if (gallopLength1 < $utils:T.INITIAL_GALLOP && gallopLength2 < $utils:T.INITIAL_GALLOP) {\n"
                            + "            context.minGallop += 2; // undo the possible subtraction from above\n"
                            + "            break;\n"
                            + "        }\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "while (tempCursor >= 0) {\n"
                            + "    valuesToSort.set(ii, context.temporaryValues.get(tempCursor));$finalCopyPermute:L\n"
                            + "    tempCursor--;\n"
                            + "    ii--;\n"
                            + "}\n", args).build())
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
            final Map<String, Object> args = namedArgs();
            return boundMethod("bound")
                    .addParameter(ParameterSpec.builder(keyChunks.chunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "lo")
                    .addParameter(int.class, "hi")
                    .addParameter(keyType(), "searchValue")
                    .addParameter(ParameterSpec.builder(boolean.class, "lower", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "final int compareLimit = lower ? -1 : 0; // lt or leq\n"
                            + "\n"
                            + "while (lo < hi) {\n"
                            + "    final int mid = (lo + hi) >>> 1;\n"
                            + "    final $kt:T testValue = valuesToSort.get(mid);\n"
                            + "    final boolean moveLo = doComparison(testValue, searchValue) <= compareLimit;\n"
                            + "    if (moveLo) {\n"
                            + "        // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]\n"
                            + "        lo = mid + 1;\n"
                            + "    } else {\n"
                            + "        hi = mid;\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "return lo;\n", args).build())
                    .build();
        }

        private MethodSpec emitInsertionSort() {
            final Map<String, Object> args = namedArgs();
            args.put("permutePrefix", permutePrefix());
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
                    .addCode(CodeBlock.builder().addNamed(""
                            + "// this could eventually be done with intrinsics (AVX 512/64 bits for long keys == 16 elements, and can be\n"
                            + "// combined up to 256)\n"
                            + "for (int ii = offset + 1; ii < offset + length; ++ii) {\n"
                            + "    for (int jj = ii; jj > offset && gt(valuesToSort.get(jj - 1), valuesToSort.get(jj)); jj--) {\n"
                            + "        swap($permutePrefix:LvaluesToSort, jj, jj - 1);\n"
                            + "    }\n"
                            + "}\n", args).build())
                    .build();
        }

        private MethodSpec emitSwap() {
            final Map<String, Object> args = namedArgs();
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("swap")
                    .addModifiers(Modifier.STATIC, Modifier.PRIVATE);
            if (hasPermute) {
                builder.addParameter(
                        ParameterSpec.builder(spec.permute.writableChunkOfWildcard(), "valuesToPermute").build());
            }
            builder.addParameter(ParameterSpec.builder(keyChunks.writableChunkOfWildcard(), "valuesToSort").build())
                    .addParameter(int.class, "a")
                    .addParameter(int.class, "b");
            if (hasPermute) {
                builder.addCode(CodeBlock.builder().addNamed(""
                        + "final $pt:T tempPermuteValue = valuesToPermute.get(a);\n"
                        + "final $kt:T $tempName:L = valuesToSort.get(a);\n"
                        + "\n"
                        + "valuesToPermute.set(a, valuesToPermute.get(b));\n"
                        + "valuesToSort.set(a, valuesToSort.get(b));\n"
                        + "\n"
                        + "valuesToPermute.set(b, tempPermuteValue);\n"
                        + "valuesToSort.set(b, $tempName:L);\n", args).build());
            } else {
                builder.addCode(CodeBlock.builder().addNamed(""
                        + "final $kt:T $tempName:L = valuesToSort.get(a);\n"
                        + "valuesToSort.set(a, valuesToSort.get(b));\n"
                        + "valuesToSort.set(b, $tempName:L);\n", args).build());
            }
            return builder.build();
        }
    }

    /**
     * Emits a multi-column kernel: two or more key columns compared in turn (lexicographically), permuting a chunk of
     * long row keys. All methods are static; the context implements {@code MultiColumnSortKernel}, and the array-based
     * {@code sort} bridge casts each column chunk to its specialized writable chunk type.
     */
    static final class MultiColumnKernelEmitter {
        private final KernelSpec spec;
        private final List<KeyColumn> keys;
        private final int n;

        private final TypeVariableName sortAttr = TypeVariableName.get("SORT_VALUES_ATTR", ANY);
        private final TypeVariableName permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);
        private final ClassName kernelClass;
        private final ClassName contextClass;
        private final TypeName parameterizedContext;

        MultiColumnKernelEmitter(final KernelSpec spec) {
            this.spec = spec;
            this.keys = spec.keys;
            this.n = keys.size();
            this.kernelClass = ClassName.get(spec.packageName(), spec.className());
            this.contextClass = kernelClass.nestedClass(spec.contextName());
            this.parameterizedContext = ParameterizedTypeName.get(contextClass, sortAttr, permuteAttr);
        }

        private ChunkFamily family(final int k) {
            return keys.get(k).kind.chunks;
        }

        private TypeName keyType(final int k) {
            return family(k).elementType;
        }

        private List<TypeVariableName> attrVariables() {
            return List.of(sortAttr, permuteAttr);
        }

        private TypeName writablePermuteChunk() {
            return spec.permute.writableChunkOf(permuteAttr);
        }

        private Map<String, Object> namedArgs() {
            final Map<String, Object> args = new HashMap<>();
            args.put("utils", TIMSORT_UTILS);
            for (int k = 0; k < n; ++k) {
                args.put("kt" + k, keyType(k));
            }
            return args;
        }

        /** Comma-joined per-key names, e.g. {@code current0, current1}. */
        private String tuple(final String base) {
            final StringBuilder builder = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    builder.append(", ");
                }
                builder.append(base).append(k);
            }
            return builder.toString();
        }

        private String valuesArgs() {
            return tuple("valuesToSort");
        }

        /** Comma-joined per-key expressions, e.g. the search values passed to the bound methods. */
        private String searchTuple(final java.util.function.IntFunction<String> expr) {
            final StringBuilder builder = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    builder.append(", ");
                }
                builder.append(expr.apply(k));
            }
            return builder.toString();
        }

        private String temporariesArgs() {
            final StringBuilder builder = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    builder.append(", ");
                }
                builder.append("context.temporaryValues").append(k);
            }
            return builder.toString();
        }

        /** One line per key column produced by the given function of the key ordinal. */
        private String keyLines(final java.util.function.IntFunction<String> line) {
            final StringBuilder builder = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                builder.append(line.apply(k));
            }
            return builder.toString();
        }

        TypeSpec emit() {
            final StringBuilder keyDescription = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    keyDescription.append(", ");
                }
                keyDescription.append(keys.get(k).kind.namePart);
            }
            final TypeSpec.Builder builder = TypeSpec.classBuilder(kernelClass)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addJavadoc("This implements a timsort kernel that sorts a multi-column key ($L), comparing each\n"
                            + "column in turn, while permuting a parallel chunk of long row keys.\n"
                            + "<p>\n"
                            + "<a href=\"https://bugs.python.org/file4451/timsort.txt\">bugs.python.org</a> and\n"
                            + "<a href=\"https://en.wikipedia.org/wiki/Timsort\">Wikipedia</a> do a decent job of "
                            + "describing the algorithm.\n", keyDescription.toString());

            builder.addMethod(MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addStatement("throw new $T()", UnsupportedOperationException.class)
                    .build());
            builder.addType(emitContext());
            builder.addMethod(emitCreateContext());
            builder.addMethod(emitSortWithRuns());
            builder.addMethod(emitSort());
            builder.addMethod(emitTimSort());
            for (int k = 0; k < n; ++k) {
                builder.addMethod(emitDoComparisonForColumn(k));
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

        private TypeSpec emitContext() {
            final TypeSpec.Builder context = TypeSpec.classBuilder(spec.contextName())
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariables(attrVariables())
                    .addSuperinterface(ParameterizedTypeName.get(MULTI_COLUMN_SORT_KERNEL, permuteAttr));

            context.addField(int.class, "minGallop");
            context.addField(FieldSpec.builder(int.class, "runCount").initializer("0").build());
            context.addField(int[].class, "runStarts", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(int[].class, "runLengths", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(FieldSpec
                    .builder(writablePermuteChunk(), "temporaryKeys", Modifier.PRIVATE, Modifier.FINAL)
                    .build());
            for (int k = 0; k < n; ++k) {
                context.addField(FieldSpec
                        .builder(family(k).writableChunkOf(sortAttr), "temporaryValues" + k,
                                Modifier.PRIVATE, Modifier.FINAL)
                        .build());
            }

            final MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(int.class, "size")
                    .addStatement("temporaryKeys = $T.makeWritableChunk((size + 2) / 2)",
                            spec.permute.writableChunkName);
            for (int k = 0; k < n; ++k) {
                constructor.addStatement("temporaryValues$L = $T.makeWritableChunk((size + 2) / 2)", k,
                        family(k).writableChunkName);
            }
            constructor.addStatement("runStarts = new int[(size + 31) / 32]");
            constructor.addStatement("runLengths = new int[(size + 31) / 32]");
            constructor.addStatement("minGallop = $T.INITIAL_GALLOP", TIMSORT_UTILS);
            context.addMethod(constructor.build());

            final StringBuilder bridgeFormat = new StringBuilder("$T.sort(this, valuesToPermute");
            final List<Object> bridgeArgs = new ArrayList<>();
            bridgeArgs.add(kernelClass);
            for (int k = 0; k < n; ++k) {
                bridgeFormat.append(",\n        ($T) valuesToSort[").append(k).append("].$L()");
                bridgeArgs.add(family(k).writableChunkOf(sortAttr));
                bridgeArgs.add(family(k).asWritableChunkMethod());
            }
            bridgeFormat.append(")");
            context.addMethod(MethodSpec.methodBuilder("sort")
                    .addAnnotation(Override.class)
                    .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                            .addMember("value", "$S", "unchecked").build())
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(writablePermuteChunk(), "valuesToPermute")
                    .addParameter(
                            ArrayTypeName.of(
                                    ParameterizedTypeName.get(WRITABLE_CHUNK, WildcardTypeName.subtypeOf(ANY))),
                            "valuesToSort")
                    .addStatement(bridgeFormat.toString(), bridgeArgs.toArray())
                    .build());

            final MethodSpec.Builder close = MethodSpec.methodBuilder("close")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("temporaryKeys.close()");
            for (int k = 0; k < n; ++k) {
                close.addStatement("temporaryValues$L.close()", k);
            }
            context.addMethod(close.build());
            return context.build();
        }

        private MethodSpec emitCreateContext() {
            return MethodSpec.methodBuilder("createContext")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariables(attrVariables())
                    .returns(parameterizedContext)
                    .addParameter(ParameterSpec.builder(int.class, "size", Modifier.FINAL).build())
                    .addStatement("return new $T<>(size)", contextClass)
                    .build();
        }

        /** Adds the standard kernel-method parameters: context, permute chunk, and one values chunk per key column. */
        private MethodSpec.Builder kernelMethod(final String name) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context")
                    .addParameter(writablePermuteChunk(), "valuesToPermute");
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).writableChunkOf(sortAttr), "valuesToSort" + k).build());
            }
            return builder;
        }

        private MethodSpec emitSortWithRuns() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("sort")
                    .addModifiers(Modifier.STATIC)
                    .addJavadoc(KernelEmitter.SORT_WITH_RUNS_JAVADOC)
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context")
                    .addParameter(writablePermuteChunk(), "valuesToPermute");
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).writableChunkOf(sortAttr), "valuesToSort" + k).build());
            }
            return builder
                    .addParameter(ParameterizedTypeName.get(INT_CHUNK, WildcardTypeName.subtypeOf(CHUNK_POSITIONS)),
                            "offsetsIn")
                    .addParameter(ParameterizedTypeName.get(INT_CHUNK, WildcardTypeName.subtypeOf(CHUNK_LENGTHS)),
                            "lengthsIn")
                    .addCode(CodeBlock.builder().add(""
                            + "final int numberRuns = offsetsIn.size();\n"
                            + "for (int run = 0; run < numberRuns; ++run) {\n"
                            + "    final int offset = offsetsIn.get(run);\n"
                            + "    final int length = lengthsIn.get(run);\n"
                            + "\n"
                            + "    timSort(context, valuesToPermute, " + valuesArgs() + ", offset, length);\n"
                            + "}\n").build())
                    .build();
        }

        private MethodSpec emitSort() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("sort")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addJavadoc("Sort the values in the valuesToSort chunks, comparing each column in turn, permuting "
                            + "the valuesToPermute\nchunk in the same way.\n")
                    .addTypeVariables(attrVariables())
                    .addParameter(parameterizedContext, "context")
                    .addParameter(writablePermuteChunk(), "valuesToPermute");
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).writableChunkOf(sortAttr), "valuesToSort" + k).build());
            }
            return builder
                    .addStatement(
                            "timSort(context, valuesToPermute, " + valuesArgs() + ", 0, valuesToPermute.size())")
                    .build();
        }

        private MethodSpec emitTimSort() {
            final java.util.function.IntFunction<String> nextExpr =
                    k -> k == 0 ? "next0" : "valuesToSort" + k + ".get(endRun)";
            final java.util.function.IntFunction<String> currentExpr =
                    k -> k == 0 ? "current0" : "valuesToSort" + k + ".get(endRun - 1)";
            // continue extending the run while the comparison holds; geq runs break on cmp < 0, strictly-descending
            // runs break on cmp >= 0
            final java.util.function.Function<String, String> runSearch = breakOp -> ""
                    + "            current0 = next0;\n"
                    + "            while (endRun < length) {\n"
                    + "                next0 = valuesToSort0.get(endRun);\n"
                    + lazyCompare("                ", "runCmp", nextExpr, currentExpr)
                    + "                if (runCmp " + breakOp + " 0) {\n"
                    + "                    break;\n"
                    + "                }\n"
                    + "                current0 = next0;\n"
                    + "                endRun++;\n"
                    + "            }\n";
            return kernelMethod("timSort")
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addCode(CodeBlock.builder().addNamed(""
                            + "if (length <= 1) {\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "final int minRun = $utils:T.getRunLength(length);\n"
                            + "\n"
                            + "if (length <= minRun) {\n"
                            + "    insertionSort(valuesToPermute, " + valuesArgs() + ", offset, length);\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "context.runCount = 0;\n"
                            + "\n"
                            + "int startRun = offset;\n"
                            + "while (startRun < offset + length) {\n"
                            + "    $kt0:T current0 = valuesToSort0.get(startRun);\n"
                            + "\n"
                            + "    int endRun; // note that endrun is exclusive\n"
                            + "    final boolean descending;\n"
                            + "\n"
                            + "    if (startRun + 1 == offset + length) {\n"
                            + "        endRun = offset + length;\n"
                            + "        descending = false;\n"
                            + "    } else {\n"
                            + "        $kt0:T next0 = valuesToSort0.get(startRun + 1);\n"
                            + "        endRun = startRun + 2;\n"
                            + lazyCompare("        ", "startCmp",
                                    k -> k == 0 ? "current0" : "valuesToSort" + k + ".get(startRun)",
                                    k -> k == 0 ? "next0" : "valuesToSort" + k + ".get(startRun + 1)")
                            + "        descending = startCmp > 0;\n"
                            + "\n"
                            + "        if (!descending) {\n"
                            + "            // search for a non-descending run\n"
                            + runSearch.apply("<")
                            + "        } else {\n"
                            + "            // search for a strictly descending run; we can not have any equal values, or we will break the\n"
                            + "            // sort's stability guarantee\n"
                            + runSearch.apply(">=")
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    final int foundLength = endRun - startRun;\n"
                            + "    context.runStarts[context.runCount] = startRun;\n"
                            + "    if (foundLength < minRun) {\n"
                            + "        // increase the size of the run to the minimum run\n"
                            + "        final int actualLength = Math.min(minRun, length - (startRun - offset));\n"
                            + "        insertionSort(valuesToPermute, " + valuesArgs() + ", startRun, actualLength);\n"
                            + "        context.runLengths[context.runCount] = actualLength;\n"
                            + "        startRun += actualLength;\n"
                            + "    } else {\n"
                            + "        if (descending) {\n"
                            + "            // reverse the current run\n"
                            + "            for (int ii = 0; ii < foundLength / 2; ++ii) {\n"
                            + "                swap(valuesToPermute, " + valuesArgs()
                            + ", ii + startRun, endRun - ii - 1);\n"
                            + "            }\n"
                            + "        }\n"
                            + "        // now an ascending run\n"
                            + "        context.runLengths[context.runCount] = foundLength;\n"
                            + "        startRun = endRun;\n"
                            + "    }\n"
                            + "\n"
                            + "    context.runCount++;\n"
                            + "\n"
                            + "    // check the invariants at the top of the stack\n"
                            + "    ensureMergeInvariants(context, valuesToPermute, " + valuesArgs() + ");\n"
                            + "}\n"
                            + "\n"
                            + "while (context.runCount > 1) {\n"
                            + "    final int length2 = context.runLengths[context.runCount - 1];\n"
                            + "    final int start1 = context.runStarts[context.runCount - 2];\n"
                            + "    final int length1 = context.runLengths[context.runCount - 2];\n"
                            + "    merge(context, valuesToPermute, " + valuesArgs()
                            + ", start1, length1, length2);\n"
                            + "    context.runStarts[context.runCount - 2] = start1;\n"
                            + "    context.runLengths[context.runCount - 2] = length1 + length2;\n"
                            + "    context.runCount--;\n"
                            + "}\n", namedArgs()).build())
                    .build();
        }

        private MethodSpec emitDoComparisonForColumn(final int k) {
            return MethodSpec.methodBuilder("doComparison" + k)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class)
                    .addParameter(keyType(k), "lhs")
                    .addParameter(keyType(k), "rhs")
                    .addStatement("return $L", keys.get(k).kind.comparison.comparisonExpression(keys.get(k).direction))
                    .build();
        }

        /**
         * Emits statements computing {@code int cmpVar} by comparing the first key column and only evaluating (and thus
         * only reading from the chunks of) later key columns when all earlier columns compare equal. This keeps
         * secondary-column data out of the cache whenever the leading column decides the comparison.
         */
        private String lazyCompare(final String indent, final String cmpVar,
                final java.util.function.IntFunction<String> lhs, final java.util.function.IntFunction<String> rhs) {
            final StringBuilder builder = new StringBuilder();
            builder.append(indent).append("int ").append(cmpVar).append(" = doComparison0(").append(lhs.apply(0))
                    .append(", ").append(rhs.apply(0)).append(");\n");
            for (int k = 1; k < n; ++k) {
                builder.append(indent).append("if (").append(cmpVar).append(" == 0) {\n");
                builder.append(indent).append("    ").append(cmpVar).append(" = doComparison").append(k).append("(")
                        .append(lhs.apply(k)).append(", ").append(rhs.apply(k)).append(");\n");
                builder.append(indent).append("}\n");
            }
            return builder.toString();
        }

        /** One line per key column after the first, produced by the given function of the key ordinal. */
        private String secondaryKeyLines(final java.util.function.IntFunction<String> line) {
            final StringBuilder builder = new StringBuilder();
            for (int k = 1; k < n; ++k) {
                builder.append(line.apply(k));
            }
            return builder.toString();
        }

        private MethodSpec emitEnsureMergeInvariants() {
            return kernelMethod("ensureMergeInvariants")
                    .addCode(CodeBlock.builder().add(""
                            + "while (context.runCount > 1) {\n"
                            + "    final int xIndex = context.runCount - 1;\n"
                            + "    final int yIndex = context.runCount - 2;\n"
                            + "    final int zIndex = context.runCount - 3;\n"
                            + "\n"
                            + "    final int xLen = context.runLengths[xIndex];\n"
                            + "    final int yLen = context.runLengths[yIndex];\n"
                            + "    final int zLen = zIndex >= 0 ? context.runLengths[zIndex] : -1;\n"
                            + "\n"
                            + "    final boolean xMerge;\n"
                            + "\n"
                            + "    if (zLen >= 0 && (zLen <= yLen + xLen)) {\n"
                            + "        // we must merge the smaller of the two\n"
                            + "        xMerge = xLen < zLen;\n"
                            + "    } else if (yLen < xLen) {\n"
                            + "        // we must merge Y into X\n"
                            + "        xMerge = true;\n"
                            + "    } else {\n"
                            + "        break;\n"
                            + "    }\n"
                            + "\n"
                            + "    final int yStart = context.runStarts[yIndex];\n"
                            + "    final int xStart = context.runStarts[xIndex];\n"
                            + "    if (xMerge) {\n"
                            + "        // merge y and x\n"
                            + "        merge(context, valuesToPermute, " + valuesArgs() + ", yStart, yLen, xLen);\n"
                            + "\n"
                            + "        // unchanged: context.runStarts[yStart];\n"
                            + "        context.runLengths[yIndex] += xLen;\n"
                            + "    } else {\n"
                            + "        // merge y and z\n"
                            + "        final int zStart = context.runStarts[zIndex];\n"
                            + "        merge(context, valuesToPermute, " + valuesArgs() + ", zStart, zLen, yLen);\n"
                            + "\n"
                            + "        // unchanged: context.runStarts[zIndex];\n"
                            + "        context.runLengths[zIndex] += yLen;\n"
                            + "        context.runStarts[yIndex] = xStart;\n"
                            + "        context.runLengths[yIndex] = xLen;\n"
                            + "    }\n"
                            + "    context.runCount--;\n"
                            + "}\n").build())
                    .build();
        }

        private MethodSpec emitMerge() {
            return kernelMethod("merge")
                    .addParameter(int.class, "start1")
                    .addParameter(int.class, "length1")
                    .addParameter(int.class, "length2")
                    .addCode(CodeBlock.builder().addNamed(""
                            + "// we know that we can never have zero length runs, because there is a minimum run size enforced; and at the\n"
                            + "// end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus\n"
                            + "// they'll never be empty. I'm being cheap about function calls and control flow here.\n"
                            + "\n"
                            + "final int start2 = start1 + length1;\n"
                            + "// find the location of run2[0] in run1\n"
                            + keyLines(k -> "final $kt" + k + ":T run2lo" + k + " = valuesToSort" + k
                                    + ".get(start2);\n")
                            + "final int mergeStartPosition = upperBound(" + valuesArgs()
                            + ", start1, start1 + length1, " + tuple("run2lo") + ");\n"
                            + "\n"
                            + "if (mergeStartPosition == start1 + length1) {\n"
                            + "    // these two runs are sorted already\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "// find the location of run1[length1 - 1] in run2\n"
                            + keyLines(k -> "final $kt" + k + ":T run1hi" + k + " = valuesToSort" + k
                                    + ".get(start1 + length1 - 1);\n")
                            + "final int mergeEndPosition = lowerBound(" + valuesArgs() + ", start2, start2 + length2, "
                            + tuple("run1hi") + ");\n"
                            + "\n"
                            + "// figure out which of the two runs is now shorter\n"
                            + "final int remaining1 = start1 + length1 - mergeStartPosition;\n"
                            + "final int remaining2 = mergeEndPosition - start2;\n"
                            + "\n"
                            + "if (remaining1 < remaining2) {\n"
                            + "    copyToTemporary(context, valuesToPermute, " + valuesArgs()
                            + ", mergeStartPosition, remaining1);\n"
                            + "    // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,\n"
                            + "    // because we've preserved all the values of run1\n"
                            + "    frontMerge(context, valuesToPermute, " + valuesArgs()
                            + ", mergeStartPosition, start2, remaining2);\n"
                            + "} else {\n"
                            + "    copyToTemporary(context, valuesToPermute, " + valuesArgs()
                            + ", start2, remaining2);\n"
                            + "    // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the\n"
                            + "    // back, because we've preserved all the values of run2)\n"
                            + "    backMerge(context, valuesToPermute, " + valuesArgs()
                            + ", mergeStartPosition, remaining1);\n"
                            + "}\n", namedArgs()).build())
                    .build();
        }

        private MethodSpec emitFrontMerge() {
            final java.util.function.IntFunction<String> run1Expr =
                    k -> k == 0 ? "val1_0" : "context.temporaryValues" + k + ".get(tempCursor)";
            final java.util.function.IntFunction<String> run2Expr =
                    k -> k == 0 ? "val2_0" : "valuesToSort" + k + ".get(run2Cursor)";
            return kernelMethod("frontMerge")
                    .addJavadoc(KernelEmitter.MERGE_JAVADOC_FRONT)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "start2", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length2", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "int tempCursor = 0;\n"
                            + "int run2Cursor = start2;\n"
                            + "\n"
                            + "final int run1size = context.temporaryValues0.size();\n"
                            + "int ii;\n"
                            + "final int mergeEndExclusive = start2 + length2;\n"
                            + "\n"
                            + "$kt0:T val1_0 = context.temporaryValues0.get(tempCursor);\n"
                            + "$kt0:T val2_0 = valuesToSort0.get(run2Cursor);\n"
                            + "\n"
                            + "ii = mergeStartPosition;\n"
                            + "\n"
                            + "nodataleft: while (ii < mergeEndExclusive) {\n"
                            + "    int run1wins = 0;\n"
                            + "    int run2wins = 0;\n"
                            + "\n"
                            + "    if (context.minGallop < 2) {\n"
                            + "        context.minGallop = 2;\n"
                            + "    }\n"
                            + "\n"
                            + "    while (run1wins < context.minGallop && run2wins < context.minGallop) {\n"
                            + lazyCompare("        ", "cmp", run1Expr, run2Expr)
                            + "        if (cmp <= 0) {\n"
                            + "            valuesToSort0.set(ii, val1_0);\n"
                            + secondaryKeyLines(k -> "            valuesToSort" + k
                                    + ".set(ii, context.temporaryValues" + k + ".get(tempCursor));\n")
                            + "            valuesToPermute.set(ii++, context.temporaryKeys.get(tempCursor));\n"
                            + "\n"
                            + "            if (++tempCursor == run1size) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "\n"
                            + "            val1_0 = context.temporaryValues0.get(tempCursor);\n"
                            + "            run1wins++;\n"
                            + "            run2wins = 0;\n"
                            + "        } else {\n"
                            + "            valuesToSort0.set(ii, val2_0);\n"
                            + secondaryKeyLines(k -> "            valuesToSort" + k + ".set(ii, valuesToSort" + k
                                    + ".get(run2Cursor));\n")
                            + "            valuesToPermute.set(ii++, valuesToPermute.get(run2Cursor));\n"
                            + "\n"
                            + "            if (++run2Cursor == mergeEndExclusive) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2_0 = valuesToSort0.get(run2Cursor);\n"
                            + "\n"
                            + "            run2wins++;\n"
                            + "            run1wins = 0;\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    // we are in galloping mode now, if we had run out of data then we should have already bailed out to\n"
                            + "    // nodataleft\n"
                            + "    while (ii < mergeEndExclusive) {\n"
                            + "        // if we had a lot of things from run1, we take the next thing from run2 then find it in run1\n"
                            + "        final int copyUntil1 = upperBound(" + temporariesArgs()
                            + ", tempCursor, run1size, " + searchTuple(run2Expr) + ");\n"
                            + "        final int gallopLength1 = copyUntil1 - tempCursor;\n"
                            + "        if (gallopLength1 > 0) {\n"
                            + "            copyToChunk(context.temporaryKeys, " + temporariesArgs()
                            + ", valuesToPermute, " + valuesArgs() + ", tempCursor, ii, gallopLength1);\n"
                            + "            tempCursor += gallopLength1;\n"
                            + "            ii += gallopLength1;\n"
                            + "\n"
                            + "            if (tempCursor == run1size) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1_0 = context.temporaryValues0.get(tempCursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        // if we had a lot of things from run2, we take the next thing from run1 and then find it in run2\n"
                            + "        final int copyUntil2 = lowerBound(" + valuesArgs()
                            + ", run2Cursor, mergeEndExclusive, " + searchTuple(run1Expr) + ");\n"
                            + "        final int gallopLength2 = copyUntil2 - run2Cursor;\n"
                            + "        if (gallopLength2 > 0) {\n"
                            + "            copyToChunk(valuesToPermute, " + valuesArgs() + ", valuesToPermute, "
                            + valuesArgs() + ", run2Cursor, ii, gallopLength2);\n"
                            + "            run2Cursor += gallopLength2;\n"
                            + "            ii += gallopLength2;\n"
                            + "\n"
                            + "            if (run2Cursor == mergeEndExclusive) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2_0 = valuesToSort0.get(run2Cursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        if (gallopLength1 < $utils:T.INITIAL_GALLOP && gallopLength2 < $utils:T.INITIAL_GALLOP) {\n"
                            + "            context.minGallop += 2; // undo the possible subtraction from above\n"
                            + "            break;\n"
                            + "        }\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "while (tempCursor < run1size) {\n"
                            + keyLines(k -> "    valuesToSort" + k + ".set(ii, context.temporaryValues" + k
                                    + ".get(tempCursor));\n")
                            + "    valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));\n"
                            + "    tempCursor++;\n"
                            + "    ii++;\n"
                            + "}\n", namedArgs()).build())
                    .build();
        }

        private MethodSpec emitBackMerge() {
            final java.util.function.IntFunction<String> run1Expr =
                    k -> k == 0 ? "val1_0" : "valuesToSort" + k + ".get(run1Cursor)";
            final java.util.function.IntFunction<String> run2Expr =
                    k -> k == 0 ? "val2_0" : "context.temporaryValues" + k + ".get(tempCursor)";
            return kernelMethod("backMerge")
                    .addJavadoc(KernelEmitter.MERGE_JAVADOC_BACK)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length1", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "final int run1End = mergeStartPosition + length1;\n"
                            + "int run1Cursor = run1End - 1;\n"
                            + "int tempCursor = context.temporaryValues0.size() - 1;\n"
                            + "\n"
                            + "final int mergeLength = context.temporaryValues0.size() + length1;\n"
                            + "int ii;\n"
                            + "\n"
                            + "$kt0:T val1_0 = valuesToSort0.get(run1Cursor);\n"
                            + "$kt0:T val2_0 = context.temporaryValues0.get(tempCursor);\n"
                            + "\n"
                            + "final int mergeEnd = mergeStartPosition + mergeLength;\n"
                            + "ii = mergeEnd - 1;\n"
                            + "\n"
                            + "nodataleft: while (ii >= mergeStartPosition) {\n"
                            + "    int run1wins = 0;\n"
                            + "    int run2wins = 0;\n"
                            + "\n"
                            + "    if (context.minGallop < 2) {\n"
                            + "        context.minGallop = 2;\n"
                            + "    }\n"
                            + "\n"
                            + "    while (run1wins < context.minGallop && run2wins < context.minGallop) {\n"
                            + lazyCompare("        ", "cmp", run2Expr, run1Expr)
                            + "        if (cmp >= 0) {\n"
                            + "            valuesToSort0.set(ii, val2_0);\n"
                            + secondaryKeyLines(k -> "            valuesToSort" + k
                                    + ".set(ii, context.temporaryValues" + k + ".get(tempCursor));\n")
                            + "            valuesToPermute.set(ii--, context.temporaryKeys.get(tempCursor));\n"
                            + "\n"
                            + "            if (--tempCursor < 0) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2_0 = context.temporaryValues0.get(tempCursor);\n"
                            + "\n"
                            + "            run2wins++;\n"
                            + "            run1wins = 0;\n"
                            + "        } else {\n"
                            + "            valuesToSort0.set(ii, val1_0);\n"
                            + secondaryKeyLines(k -> "            valuesToSort" + k + ".set(ii, valuesToSort" + k
                                    + ".get(run1Cursor));\n")
                            + "            valuesToPermute.set(ii--, valuesToPermute.get(run1Cursor));\n"
                            + "\n"
                            + "            if (--run1Cursor < mergeStartPosition) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1_0 = valuesToSort0.get(run1Cursor);\n"
                            + "\n"
                            + "            run1wins++;\n"
                            + "            run2wins = 0;\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    // we are in galloping mode now, if we had run out of data then we should have already bailed out to\n"
                            + "    // nodataleft\n"
                            + "    while (ii >= mergeStartPosition) {\n"
                            + "        // if we had a lot of things from run2, we take the next thing from run1 then find it in run2\n"
                            + "        final int copyUntil2 = lowerBound(" + temporariesArgs() + ", 0, tempCursor, "
                            + searchTuple(run1Expr) + ") + 1;\n"
                            + "\n"
                            + "        final int gallopLength2 = tempCursor - copyUntil2 + 1;\n"
                            + "        if (gallopLength2 > 0) {\n"
                            + "            copyToChunk(context.temporaryKeys, " + temporariesArgs()
                            + ", valuesToPermute, " + valuesArgs()
                            + ", copyUntil2, ii - gallopLength2 + 1, gallopLength2);\n"
                            + "            tempCursor -= gallopLength2;\n"
                            + "            ii -= gallopLength2;\n"
                            + "\n"
                            + "            if (tempCursor < 0) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2_0 = context.temporaryValues0.get(tempCursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1\n"
                            + "        final int copyUntil1 = upperBound(" + valuesArgs()
                            + ", mergeStartPosition, run1Cursor, " + searchTuple(run2Expr) + ");\n"
                            + "\n"
                            + "        final int gallopLength1 = run1Cursor - copyUntil1;\n"
                            + "        if (gallopLength1 > 0) {\n"
                            + "            copyToChunk(valuesToPermute, " + valuesArgs() + ", valuesToPermute, "
                            + valuesArgs() + ", copyUntil1, ii - gallopLength1, gallopLength1 + 1);\n"
                            + "            run1Cursor -= gallopLength1;\n"
                            + "            ii -= gallopLength1;\n"
                            + "\n"
                            + "            if (run1Cursor < mergeStartPosition) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1_0 = valuesToSort0.get(run1Cursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        if (gallopLength1 < $utils:T.INITIAL_GALLOP && gallopLength2 < $utils:T.INITIAL_GALLOP) {\n"
                            + "            context.minGallop += 2; // undo the possible subtraction from above\n"
                            + "            break;\n"
                            + "        }\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "while (tempCursor >= 0) {\n"
                            + keyLines(k -> "    valuesToSort" + k + ".set(ii, context.temporaryValues" + k
                                    + ".get(tempCursor));\n")
                            + "    valuesToPermute.set(ii, context.temporaryKeys.get(tempCursor));\n"
                            + "    tempCursor--;\n"
                            + "    ii--;\n"
                            + "}\n", namedArgs()).build())
                    .build();
        }

        private MethodSpec emitCopyToTemporary() {
            final MethodSpec.Builder builder = kernelMethod("copyToTemporary")
                    .addParameter(int.class, "mergeStartPosition")
                    .addParameter(int.class, "remaining1");
            for (int k = 0; k < n; ++k) {
                builder.addStatement("context.temporaryValues$L.setSize(remaining1)", k);
            }
            builder.addStatement("context.temporaryKeys.setSize(remaining1)");
            for (int k = 0; k < n; ++k) {
                builder.addStatement(
                        "context.temporaryValues$L.copyFromChunk(valuesToSort$L, mergeStartPosition, 0, remaining1)",
                        k, k);
            }
            builder.addStatement("context.temporaryKeys.copyFromChunk(valuesToPermute, mergeStartPosition, 0, "
                    + "remaining1)");
            return builder.build();
        }

        private MethodSpec emitCopyToChunk() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("copyToChunk")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addTypeVariables(attrVariables())
                    .addParameter(ParameterSpec.builder(spec.permute.chunkOf(permuteAttr), "rowSetSource").build());
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).chunkOf(sortAttr), "valuesSource" + k).build());
            }
            builder.addParameter(ParameterSpec.builder(writablePermuteChunk(), "permuteValuesDest").build());
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).writableChunkOf(sortAttr), "sortValuesDest" + k).build());
            }
            builder.addParameter(int.class, "sourceStart")
                    .addParameter(int.class, "destStart")
                    .addParameter(int.class, "length");
            for (int k = 0; k < n; ++k) {
                builder.addStatement("sortValuesDest$L.copyFromChunk(valuesSource$L, sourceStart, destStart, length)",
                        k, k);
            }
            builder.addStatement("permuteValuesDest.copyFromChunk(rowSetSource, sourceStart, destStart, length)");
            return builder.build();
        }

        /** The bound methods take wildcard-attribute chunks, as they search temporaries and inputs alike. */
        private MethodSpec.Builder boundMethod(final String name) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class);
            for (int k = 0; k < n; ++k) {
                builder.addParameter(ParameterSpec.builder(family(k).chunkOfWildcard(), "valuesToSort" + k).build());
            }
            builder.addParameter(int.class, "lo")
                    .addParameter(int.class, "hi");
            for (int k = 0; k < n; ++k) {
                builder.addParameter(keyType(k), "searchValue" + k);
            }
            return builder;
        }

        private MethodSpec emitUpperBound() {
            return boundMethod("upperBound")
                    .addComment(
                            "when we binary search in 1, we must identify a position for search value that is *after* our test values;")
                    .addComment("because the values from run 2 may never be inserted before an equal value from run 1")
                    .addComment("")
                    .addComment("lo is inclusive, hi is exclusive")
                    .addComment("")
                    .addComment(
                            "returns the position of the first element that is > searchValue or hi if there is no such element")
                    .addStatement("return bound(" + valuesArgs() + ", lo, hi, " + tuple("searchValue") + ", false)")
                    .build();
        }

        private MethodSpec emitLowerBound() {
            return boundMethod("lowerBound")
                    .addComment(
                            "when we binary search in 2, we must identify a position for search value that is *before* our test values;")
                    .addComment("because the values from run 1 may never be inserted after an equal value from run 2")
                    .addStatement("return bound(" + valuesArgs() + ", lo, hi, " + tuple("searchValue") + ", true)")
                    .build();
        }

        private MethodSpec emitBound() {
            return boundMethod("bound")
                    .addParameter(ParameterSpec.builder(boolean.class, "lower", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "final int compareLimit = lower ? -1 : 0; // lt or leq\n"
                            + "\n"
                            + "while (lo < hi) {\n"
                            + "    final int mid = (lo + hi) >>> 1;\n"
                            + lazyCompare("    ", "cmp",
                                    k -> "valuesToSort" + k + ".get(mid)",
                                    k -> "searchValue" + k)
                            + "    final boolean moveLo = cmp <= compareLimit;\n"
                            + "    if (moveLo) {\n"
                            + "        // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]\n"
                            + "        lo = mid + 1;\n"
                            + "    } else {\n"
                            + "        hi = mid;\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "return lo;\n", namedArgs()).build())
                    .build();
        }

        private MethodSpec emitInsertionSort() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("insertionSort")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addParameter(
                            ParameterSpec.builder(spec.permute.writableChunkOfWildcard(), "valuesToPermute").build());
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).writableChunkOfWildcard(), "valuesToSort" + k).build());
            }
            return builder
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addCode(CodeBlock.builder().add(""
                            + "for (int ii = offset + 1; ii < offset + length; ++ii) {\n"
                            + "    for (int jj = ii; jj > offset; jj--) {\n"
                            + lazyCompare("        ", "cmp",
                                    k -> "valuesToSort" + k + ".get(jj - 1)",
                                    k -> "valuesToSort" + k + ".get(jj)")
                            + "        if (cmp <= 0) {\n"
                            + "            break;\n"
                            + "        }\n"
                            + "        swap(valuesToPermute, " + valuesArgs() + ", jj, jj - 1);\n"
                            + "    }\n"
                            + "}\n").build())
                    .build();
        }

        private MethodSpec emitSwap() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("swap")
                    .addModifiers(Modifier.STATIC, Modifier.PRIVATE)
                    .addParameter(
                            ParameterSpec.builder(spec.permute.writableChunkOfWildcard(), "valuesToPermute").build());
            for (int k = 0; k < n; ++k) {
                builder.addParameter(
                        ParameterSpec.builder(family(k).writableChunkOfWildcard(), "valuesToSort" + k).build());
            }
            builder.addParameter(int.class, "a")
                    .addParameter(int.class, "b");
            builder.addCode(CodeBlock.builder().addNamed(""
                    + "final long tempPermuteValue = valuesToPermute.get(a);\n"
                    + keyLines(k -> "final $kt" + k + ":T temp" + k + " = valuesToSort" + k + ".get(a);\n")
                    + "\n"
                    + "valuesToPermute.set(a, valuesToPermute.get(b));\n"
                    + keyLines(k -> "valuesToSort" + k + ".set(a, valuesToSort" + k + ".get(b));\n")
                    + "\n"
                    + "valuesToPermute.set(b, tempPermuteValue);\n"
                    + keyLines(k -> "valuesToSort" + k + ".set(b, temp" + k + ");\n"), namedArgs()).build());
            return builder.build();
        }
    }

    /**
     * Emits the dispatcher that selects a generated multi-column kernel for an array of chunk types and sort
     * directions, or returns null when no specialized kernel exists (wrong arity, descending columns, or boolean
     * columns) so the caller can fall back to the single-column pipeline.
     */
    private static void generateMultiColumnDispatcher(final boolean indirect) throws IOException {
        final String kernelSuffix = indirect ? "IndirectMultiColumnTimsortKernel" : "MultiColumnTimsortKernel";
        final String dispatcherName =
                indirect ? "IndirectMultiColumnTimsortDispatcher" : "MultiColumnTimsortDispatcher";
        final TypeVariableName permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);

        final MethodSpec.Builder makeContext = MethodSpec.methodBuilder("makeContext")
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addTypeVariable(permuteAttr)
                .returns(ParameterizedTypeName.get(MULTI_COLUMN_SORT_KERNEL, permuteAttr))
                .addParameter(ArrayTypeName.of(CHUNK_TYPE), "chunkTypes")
                .addParameter(ArrayTypeName.of(SORTING_ORDER), "order")
                .addParameter(int.class, "size");

        makeContext.beginControlFlow("for (final $T columnOrder : order)", SORTING_ORDER);
        makeContext.beginControlFlow("if (columnOrder != $T.Ascending)", SORTING_ORDER);
        makeContext.addStatement("return null");
        makeContext.endControlFlow();
        makeContext.endControlFlow();

        if (indirect) {
            makeContext.beginControlFlow("if (chunkTypes.length == 1)");
            makeContext.beginControlFlow("switch (chunkTypes[0])");
            for (final KeyKind kind : ENGINE_KINDS) {
                makeContext.addCode("case " + kind.chunks.name + ": ");
                makeContext.addStatement("return $T.createContext(size)",
                        ClassName.get(MULTI_PACKAGE, kind.namePart + "IndirectTimsortKernel"));
            }
            makeContext.addCode("default: ");
            makeContext.addStatement("return null");
            makeContext.endControlFlow();
            makeContext.endControlFlow();
        }

        makeContext.beginControlFlow("if (chunkTypes.length != 2)");
        makeContext.addStatement("return null");
        makeContext.endControlFlow();

        makeContext.beginControlFlow("switch (chunkTypes[0])");
        for (final KeyKind kind0 : ENGINE_KINDS) {
            makeContext.addCode("case " + kind0.chunks.name + ":\n");
            makeContext.beginControlFlow("switch (chunkTypes[1])");
            for (final KeyKind kind1 : ENGINE_KINDS) {
                makeContext.addCode("case " + kind1.chunks.name + ": ");
                makeContext.addStatement("return $T.createContext(size)",
                        ClassName.get(MULTI_PACKAGE, kind0.namePart + kind1.namePart + kernelSuffix));
            }
            makeContext.addCode("default: ");
            makeContext.addStatement("return null");
            makeContext.endControlFlow();
        }
        makeContext.addCode("default: ");
        makeContext.addStatement("return null");
        makeContext.endControlFlow();

        final TypeSpec dispatcher = TypeSpec.classBuilder(dispatcherName)
                .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                .addJavadoc("Selects a generated " + (indirect ? "indirect (position-permuting) " : "")
                        + "multi-column timsort kernel for the given column chunk types and "
                        + "sort directions,\nreturning null when no specialized kernel exists so callers can fall "
                        + "back to sorting one column at a\ntime.\n")
                .addMethod(MethodSpec.constructorBuilder().addModifiers(Modifier.PRIVATE).build())
                .addMethod(makeContext.build())
                .build();
        writeFile(MULTI_PACKAGE, dispatcher, dispatcherName);
    }

    /**
     * Emits an indirect multi-column kernel: the timsort permutes only a parallel chunk of int positions, reading the
     * (never moved) column values through those positions for each lazy per-column comparison. The context's
     * array-based sort bridge initializes the identity positions, runs the sort, and then assembles the permuted row
     * keys in a single linear pass — the row keys are not permuted during the sort either.
     */
    static final class IndirectMultiColumnKernelEmitter {
        private final KernelSpec spec;
        private final List<KeyColumn> keys;
        private final int n;

        private final TypeVariableName permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);
        private final ClassName kernelClass;
        private final ClassName contextClass;
        private final TypeName wildcardContext;
        private final TypeName writablePositions;
        private final TypeName readablePositions;

        IndirectMultiColumnKernelEmitter(final KernelSpec spec) {
            this.spec = spec;
            this.keys = spec.keys;
            this.n = keys.size();
            this.kernelClass = ClassName.get(spec.packageName(), spec.className());
            this.contextClass = kernelClass.nestedClass(spec.contextName());
            this.wildcardContext =
                    ParameterizedTypeName.get(contextClass, WildcardTypeName.subtypeOf(ClassName.OBJECT));
            this.writablePositions =
                    ParameterizedTypeName.get(INT_CHUNKS.writableChunkName, CHUNK_POSITIONS);
            this.readablePositions = ParameterizedTypeName.get(INT_CHUNK, CHUNK_POSITIONS);
        }

        private ChunkFamily family(final int k) {
            return keys.get(k).kind.chunks;
        }

        private String valuesArgs() {
            final StringBuilder builder = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    builder.append(", ");
                }
                builder.append("valuesToSort").append(k);
            }
            return builder.toString();
        }

        /** A lazy per-column comparison of the elements at two positions. */
        private String compareCall(final String lhsPos, final String rhsPos) {
            return "compareColumns(" + valuesArgs() + ", " + lhsPos + ", " + rhsPos + ")";
        }

        private void addValuesParams(final MethodSpec.Builder builder) {
            for (int k = 0; k < n; ++k) {
                builder.addParameter(ParameterSpec.builder(family(k).chunkOfWildcard(), "valuesToSort" + k).build());
            }
        }

        TypeSpec emit() {
            final StringBuilder keyDescription = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    keyDescription.append(", ");
                }
                keyDescription.append(keys.get(k).kind.namePart);
            }
            final TypeSpec.Builder builder = TypeSpec.classBuilder(kernelClass)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addJavadoc("This implements a timsort kernel for a multi-column key ($L) that never moves the "
                            + "column values:\nit permutes a parallel chunk of int positions, reading values through "
                            + "the positions for each\ncomparison (comparing each column in turn, only reading later "
                            + "columns on ties). The row keys are\nnot permuted during the sort either; they are "
                            + "assembled in a single linear pass at the end.\n"
                            + "<p>\n"
                            + "<a href=\"https://bugs.python.org/file4451/timsort.txt\">bugs.python.org</a> and\n"
                            + "<a href=\"https://en.wikipedia.org/wiki/Timsort\">Wikipedia</a> do a decent job of "
                            + "describing the algorithm.\n", keyDescription.toString());

            builder.addMethod(MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addStatement("throw new $T()", UnsupportedOperationException.class)
                    .build());
            builder.addType(emitContext());
            builder.addMethod(emitCreateContext());
            builder.addMethod(emitSort());
            builder.addMethod(emitTimSort());
            for (int k = 0; k < n; ++k) {
                builder.addMethod(emitDoComparisonForColumn(k));
            }
            builder.addMethod(emitCompareColumns());
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

        private TypeSpec emitContext() {
            final TypeSpec.Builder context = TypeSpec.classBuilder(spec.contextName())
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(permuteAttr)
                    .addSuperinterface(ParameterizedTypeName.get(MULTI_COLUMN_SORT_KERNEL, permuteAttr));

            context.addField(int.class, "minGallop");
            context.addField(FieldSpec.builder(int.class, "runCount").initializer("0").build());
            context.addField(int[].class, "runStarts", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(int[].class, "runLengths", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(FieldSpec.builder(writablePositions, "positions", Modifier.PRIVATE, Modifier.FINAL)
                    .build());
            context.addField(
                    FieldSpec.builder(writablePositions, "temporaryPositions", Modifier.PRIVATE, Modifier.FINAL)
                            .build());
            context.addField(FieldSpec
                    .builder(spec.permute.writableChunkOf(permuteAttr), "temporaryKeys",
                            Modifier.PRIVATE, Modifier.FINAL)
                    .build());

            context.addMethod(MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(int.class, "size")
                    .addStatement("positions = $T.makeWritableChunk(size)", INT_CHUNKS.writableChunkName)
                    .addStatement("temporaryPositions = $T.makeWritableChunk((size + 2) / 2)",
                            INT_CHUNKS.writableChunkName)
                    .addStatement("temporaryKeys = $T.makeWritableChunk(size)", spec.permute.writableChunkName)
                    .addStatement("runStarts = new int[(size + 31) / 32]")
                    .addStatement("runLengths = new int[(size + 31) / 32]")
                    .addStatement("minGallop = $T.INITIAL_GALLOP", TIMSORT_UTILS)
                    .build());

            final StringBuilder sortFormat = new StringBuilder("$T.sort(this, positions");
            final List<Object> sortArgs = new ArrayList<>();
            sortArgs.add(kernelClass);
            for (int k = 0; k < n; ++k) {
                if (family(k).isObject) {
                    sortFormat.append(", valuesToSort[").append(k).append("].<$T>as").append(family(k).name)
                            .append("Chunk()");
                    sortArgs.add(ClassName.OBJECT);
                } else {
                    sortFormat.append(", valuesToSort[").append(k).append("].as").append(family(k).name)
                            .append("Chunk()");
                }
            }
            sortFormat.append(")");
            context.addMethod(MethodSpec.methodBuilder("sort")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(spec.permute.writableChunkOf(permuteAttr), "valuesToPermute")
                    .addParameter(
                            ArrayTypeName.of(
                                    ParameterizedTypeName.get(WRITABLE_CHUNK, WildcardTypeName.subtypeOf(ANY))),
                            "valuesToSort")
                    .addStatement("final int size = valuesToPermute.size()")
                    .addStatement("positions.setSize(size)")
                    .beginControlFlow("for (int ii = 0; ii < size; ++ii)")
                    .addStatement("positions.set(ii, ii)")
                    .endControlFlow()
                    .addStatement(sortFormat.toString(), sortArgs.toArray())
                    .addComment(
                            "assemble the permuted row keys in a single linear pass rather than permuting them during the sort")
                    .addStatement("temporaryKeys.copyFromChunk(valuesToPermute, 0, 0, size)")
                    .beginControlFlow("for (int ii = 0; ii < size; ++ii)")
                    .addStatement("valuesToPermute.set(ii, temporaryKeys.get(positions.get(ii)))")
                    .endControlFlow()
                    .build());

            context.addMethod(MethodSpec.methodBuilder("close")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("positions.close()")
                    .addStatement("temporaryPositions.close()")
                    .addStatement("temporaryKeys.close()")
                    .build());
            return context.build();
        }

        private MethodSpec emitCreateContext() {
            return MethodSpec.methodBuilder("createContext")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(permuteAttr)
                    .returns(ParameterizedTypeName.get(contextClass, permuteAttr))
                    .addParameter(ParameterSpec.builder(int.class, "size", Modifier.FINAL).build())
                    .addStatement("return new $T<>(size)", contextClass)
                    .build();
        }

        /** Adds the standard kernel-method parameters: context, positions chunk, and the value chunks. */
        private MethodSpec.Builder kernelMethod(final String name) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addParameter(wildcardContext, "context")
                    .addParameter(writablePositions, "positions");
            addValuesParams(builder);
            return builder;
        }

        private MethodSpec emitSort() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("sort")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addJavadoc("Sort the positions chunk such that the values it points to are lexicographically "
                            + "ordered, comparing\neach column in turn; the value chunks themselves are not modified.\n")
                    .addParameter(wildcardContext, "context")
                    .addParameter(writablePositions, "positions");
            addValuesParams(builder);
            return builder
                    .addStatement("timSort(context, positions, " + valuesArgs() + ", 0, positions.size())")
                    .build();
        }

        private MethodSpec emitTimSort() {
            return kernelMethod("timSort")
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addCode(CodeBlock.builder().addNamed(""
                            + "if (length <= 1) {\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "final int minRun = $utils:T.getRunLength(length);\n"
                            + "\n"
                            + "if (length <= minRun) {\n"
                            + "    insertionSort(positions, " + valuesArgs() + ", offset, length);\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "context.runCount = 0;\n"
                            + "\n"
                            + "int startRun = offset;\n"
                            + "while (startRun < offset + length) {\n"
                            + "    int currentPos = positions.get(startRun);\n"
                            + "\n"
                            + "    int endRun; // note that endrun is exclusive\n"
                            + "    final boolean descending;\n"
                            + "\n"
                            + "    if (startRun + 1 == offset + length) {\n"
                            + "        endRun = offset + length;\n"
                            + "        descending = false;\n"
                            + "    } else {\n"
                            + "        int nextPos = positions.get(startRun + 1);\n"
                            + "        endRun = startRun + 2;\n"
                            + "        descending = " + compareCall("currentPos", "nextPos") + " > 0;\n"
                            + "\n"
                            + "        if (!descending) {\n"
                            + "            // search for a non-descending run\n"
                            + "            currentPos = nextPos;\n"
                            + "            while (endRun < length) {\n"
                            + "                nextPos = positions.get(endRun);\n"
                            + "                if (" + compareCall("nextPos", "currentPos") + " < 0) {\n"
                            + "                    break;\n"
                            + "                }\n"
                            + "                currentPos = nextPos;\n"
                            + "                endRun++;\n"
                            + "            }\n"
                            + "        } else {\n"
                            + "            // search for a strictly descending run; we can not have any equal values, or we will break the\n"
                            + "            // sort's stability guarantee\n"
                            + "            currentPos = nextPos;\n"
                            + "            while (endRun < length) {\n"
                            + "                nextPos = positions.get(endRun);\n"
                            + "                if (" + compareCall("nextPos", "currentPos") + " >= 0) {\n"
                            + "                    break;\n"
                            + "                }\n"
                            + "                currentPos = nextPos;\n"
                            + "                endRun++;\n"
                            + "            }\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    final int foundLength = endRun - startRun;\n"
                            + "    context.runStarts[context.runCount] = startRun;\n"
                            + "    if (foundLength < minRun) {\n"
                            + "        // increase the size of the run to the minimum run\n"
                            + "        final int actualLength = Math.min(minRun, length - (startRun - offset));\n"
                            + "        insertionSort(positions, " + valuesArgs() + ", startRun, actualLength);\n"
                            + "        context.runLengths[context.runCount] = actualLength;\n"
                            + "        startRun += actualLength;\n"
                            + "    } else {\n"
                            + "        if (descending) {\n"
                            + "            // reverse the current run\n"
                            + "            for (int ii = 0; ii < foundLength / 2; ++ii) {\n"
                            + "                swap(positions, ii + startRun, endRun - ii - 1);\n"
                            + "            }\n"
                            + "        }\n"
                            + "        // now an ascending run\n"
                            + "        context.runLengths[context.runCount] = foundLength;\n"
                            + "        startRun = endRun;\n"
                            + "    }\n"
                            + "\n"
                            + "    context.runCount++;\n"
                            + "\n"
                            + "    // check the invariants at the top of the stack\n"
                            + "    ensureMergeInvariants(context, positions, " + valuesArgs() + ");\n"
                            + "}\n"
                            + "\n"
                            + "while (context.runCount > 1) {\n"
                            + "    final int length2 = context.runLengths[context.runCount - 1];\n"
                            + "    final int start1 = context.runStarts[context.runCount - 2];\n"
                            + "    final int length1 = context.runLengths[context.runCount - 2];\n"
                            + "    merge(context, positions, " + valuesArgs() + ", start1, length1, length2);\n"
                            + "    context.runStarts[context.runCount - 2] = start1;\n"
                            + "    context.runLengths[context.runCount - 2] = length1 + length2;\n"
                            + "    context.runCount--;\n"
                            + "}\n", Map.of("utils", TIMSORT_UTILS)).build())
                    .build();
        }

        private MethodSpec emitDoComparisonForColumn(final int k) {
            return MethodSpec.methodBuilder("doComparison" + k)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class)
                    .addParameter(family(k).elementType, "lhs")
                    .addParameter(family(k).elementType, "rhs")
                    .addStatement("return $L", keys.get(k).kind.comparison.comparisonExpression(keys.get(k).direction))
                    .build();
        }

        private MethodSpec emitCompareColumns() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("compareColumns")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class)
                    .addJavadoc("Compares the elements at two positions, column by column; later columns are only "
                            + "read when all\nearlier columns compare equal.\n");
            addValuesParams(builder);
            builder.addParameter(int.class, "lhsPos")
                    .addParameter(int.class, "rhsPos");
            builder.addStatement(
                    "final int cmp0 = doComparison0(valuesToSort0.get(lhsPos), valuesToSort0.get(rhsPos))");
            if (n == 1) {
                builder.addStatement("return cmp0");
                return builder.build();
            }
            for (int k = 1; k < n; ++k) {
                if (k < n - 1) {
                    builder.beginControlFlow("if (cmp$L != 0)", k - 1);
                    builder.addStatement("return cmp$L", k - 1);
                    builder.endControlFlow();
                    builder.addStatement(
                            "final int cmp$L = doComparison$L(valuesToSort$L.get(lhsPos), valuesToSort$L.get(rhsPos))",
                            k, k, k, k);
                } else {
                    builder.beginControlFlow("if (cmp$L != 0)", k - 1);
                    builder.addStatement("return cmp$L", k - 1);
                    builder.endControlFlow();
                    builder.addStatement(
                            "return doComparison$L(valuesToSort$L.get(lhsPos), valuesToSort$L.get(rhsPos))",
                            k, k, k);
                }
            }
            return builder.build();
        }

        private MethodSpec emitEnsureMergeInvariants() {
            return kernelMethod("ensureMergeInvariants")
                    .addCode(CodeBlock.builder().add(""
                            + "while (context.runCount > 1) {\n"
                            + "    final int xIndex = context.runCount - 1;\n"
                            + "    final int yIndex = context.runCount - 2;\n"
                            + "    final int zIndex = context.runCount - 3;\n"
                            + "\n"
                            + "    final int xLen = context.runLengths[xIndex];\n"
                            + "    final int yLen = context.runLengths[yIndex];\n"
                            + "    final int zLen = zIndex >= 0 ? context.runLengths[zIndex] : -1;\n"
                            + "\n"
                            + "    final boolean xMerge;\n"
                            + "\n"
                            + "    if (zLen >= 0 && (zLen <= yLen + xLen)) {\n"
                            + "        // we must merge the smaller of the two\n"
                            + "        xMerge = xLen < zLen;\n"
                            + "    } else if (yLen < xLen) {\n"
                            + "        // we must merge Y into X\n"
                            + "        xMerge = true;\n"
                            + "    } else {\n"
                            + "        break;\n"
                            + "    }\n"
                            + "\n"
                            + "    final int yStart = context.runStarts[yIndex];\n"
                            + "    final int xStart = context.runStarts[xIndex];\n"
                            + "    if (xMerge) {\n"
                            + "        // merge y and x\n"
                            + "        merge(context, positions, " + valuesArgs() + ", yStart, yLen, xLen);\n"
                            + "\n"
                            + "        // unchanged: context.runStarts[yStart];\n"
                            + "        context.runLengths[yIndex] += xLen;\n"
                            + "    } else {\n"
                            + "        // merge y and z\n"
                            + "        final int zStart = context.runStarts[zIndex];\n"
                            + "        merge(context, positions, " + valuesArgs() + ", zStart, zLen, yLen);\n"
                            + "\n"
                            + "        // unchanged: context.runStarts[zIndex];\n"
                            + "        context.runLengths[zIndex] += yLen;\n"
                            + "        context.runStarts[yIndex] = xStart;\n"
                            + "        context.runLengths[yIndex] = xLen;\n"
                            + "    }\n"
                            + "    context.runCount--;\n"
                            + "}\n").build())
                    .build();
        }

        private MethodSpec emitMerge() {
            return kernelMethod("merge")
                    .addParameter(int.class, "start1")
                    .addParameter(int.class, "length1")
                    .addParameter(int.class, "length2")
                    .addCode(CodeBlock.builder().add(""
                            + "// we know that we can never have zero length runs, because there is a minimum run size enforced; and at the\n"
                            + "// end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus\n"
                            + "// they'll never be empty. I'm being cheap about function calls and control flow here.\n"
                            + "\n"
                            + "final int start2 = start1 + length1;\n"
                            + "// find the location of run2[0] in run1\n"
                            + "final int run2loPos = positions.get(start2);\n"
                            + "final int mergeStartPosition = upperBound(positions, " + valuesArgs()
                            + ", start1, start1 + length1, run2loPos);\n"
                            + "\n"
                            + "if (mergeStartPosition == start1 + length1) {\n"
                            + "    // these two runs are sorted already\n"
                            + "    return;\n"
                            + "}\n"
                            + "\n"
                            + "// find the location of run1[length1 - 1] in run2\n"
                            + "final int run1hiPos = positions.get(start1 + length1 - 1);\n"
                            + "final int mergeEndPosition = lowerBound(positions, " + valuesArgs()
                            + ", start2, start2 + length2, run1hiPos);\n"
                            + "\n"
                            + "// figure out which of the two runs is now shorter\n"
                            + "final int remaining1 = start1 + length1 - mergeStartPosition;\n"
                            + "final int remaining2 = mergeEndPosition - start2;\n"
                            + "\n"
                            + "if (remaining1 < remaining2) {\n"
                            + "    copyToTemporary(context, positions, mergeStartPosition, remaining1);\n"
                            + "    // now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,\n"
                            + "    // because we've preserved all the values of run1\n"
                            + "    frontMerge(context, positions, " + valuesArgs()
                            + ", mergeStartPosition, start2, remaining2);\n"
                            + "} else {\n"
                            + "    copyToTemporary(context, positions, start2, remaining2);\n"
                            + "    // now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the\n"
                            + "    // back, because we've preserved all the values of run2)\n"
                            + "    backMerge(context, positions, " + valuesArgs()
                            + ", mergeStartPosition, remaining1);\n"
                            + "}\n").build())
                    .build();
        }

        private MethodSpec emitFrontMerge() {
            return kernelMethod("frontMerge")
                    .addJavadoc(KernelEmitter.MERGE_JAVADOC_FRONT)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "start2", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length2", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "int tempCursor = 0;\n"
                            + "int run2Cursor = start2;\n"
                            + "\n"
                            + "final int run1size = context.temporaryPositions.size();\n"
                            + "int ii;\n"
                            + "final int mergeEndExclusive = start2 + length2;\n"
                            + "\n"
                            + "int val1Pos = context.temporaryPositions.get(tempCursor);\n"
                            + "int val2Pos = positions.get(run2Cursor);\n"
                            + "\n"
                            + "ii = mergeStartPosition;\n"
                            + "\n"
                            + "nodataleft: while (ii < mergeEndExclusive) {\n"
                            + "    int run1wins = 0;\n"
                            + "    int run2wins = 0;\n"
                            + "\n"
                            + "    if (context.minGallop < 2) {\n"
                            + "        context.minGallop = 2;\n"
                            + "    }\n"
                            + "\n"
                            + "    while (run1wins < context.minGallop && run2wins < context.minGallop) {\n"
                            + "        if (" + compareCall("val1Pos", "val2Pos") + " <= 0) {\n"
                            + "            positions.set(ii++, val1Pos);\n"
                            + "\n"
                            + "            if (++tempCursor == run1size) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "\n"
                            + "            val1Pos = context.temporaryPositions.get(tempCursor);\n"
                            + "            run1wins++;\n"
                            + "            run2wins = 0;\n"
                            + "        } else {\n"
                            + "            positions.set(ii++, val2Pos);\n"
                            + "\n"
                            + "            if (++run2Cursor == mergeEndExclusive) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2Pos = positions.get(run2Cursor);\n"
                            + "\n"
                            + "            run2wins++;\n"
                            + "            run1wins = 0;\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    // we are in galloping mode now, if we had run out of data then we should have already bailed out to\n"
                            + "    // nodataleft\n"
                            + "    while (ii < mergeEndExclusive) {\n"
                            + "        // if we had a lot of things from run1, we take the next thing from run2 then find it in run1\n"
                            + "        final int copyUntil1 = upperBound(context.temporaryPositions, " + valuesArgs()
                            + ", tempCursor, run1size, val2Pos);\n"
                            + "        final int gallopLength1 = copyUntil1 - tempCursor;\n"
                            + "        if (gallopLength1 > 0) {\n"
                            + "            copyToChunk(context.temporaryPositions, positions, tempCursor, ii, gallopLength1);\n"
                            + "            tempCursor += gallopLength1;\n"
                            + "            ii += gallopLength1;\n"
                            + "\n"
                            + "            if (tempCursor == run1size) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1Pos = context.temporaryPositions.get(tempCursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        // if we had a lot of things from run2, we take the next thing from run1 and then find it in run2\n"
                            + "        final int copyUntil2 = lowerBound(positions, " + valuesArgs()
                            + ", run2Cursor, mergeEndExclusive, val1Pos);\n"
                            + "        final int gallopLength2 = copyUntil2 - run2Cursor;\n"
                            + "        if (gallopLength2 > 0) {\n"
                            + "            copyToChunk(positions, positions, run2Cursor, ii, gallopLength2);\n"
                            + "            run2Cursor += gallopLength2;\n"
                            + "            ii += gallopLength2;\n"
                            + "\n"
                            + "            if (run2Cursor == mergeEndExclusive) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2Pos = positions.get(run2Cursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        if (gallopLength1 < $utils:T.INITIAL_GALLOP && gallopLength2 < $utils:T.INITIAL_GALLOP) {\n"
                            + "            context.minGallop += 2; // undo the possible subtraction from above\n"
                            + "            break;\n"
                            + "        }\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "while (tempCursor < run1size) {\n"
                            + "    positions.set(ii, context.temporaryPositions.get(tempCursor));\n"
                            + "    tempCursor++;\n"
                            + "    ii++;\n"
                            + "}\n", Map.of("utils", TIMSORT_UTILS)).build())
                    .build();
        }

        private MethodSpec emitBackMerge() {
            return kernelMethod("backMerge")
                    .addJavadoc(KernelEmitter.MERGE_JAVADOC_BACK)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length1", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().addNamed(""
                            + "final int run1End = mergeStartPosition + length1;\n"
                            + "int run1Cursor = run1End - 1;\n"
                            + "int tempCursor = context.temporaryPositions.size() - 1;\n"
                            + "\n"
                            + "final int mergeLength = context.temporaryPositions.size() + length1;\n"
                            + "int ii;\n"
                            + "\n"
                            + "int val1Pos = positions.get(run1Cursor);\n"
                            + "int val2Pos = context.temporaryPositions.get(tempCursor);\n"
                            + "\n"
                            + "final int mergeEnd = mergeStartPosition + mergeLength;\n"
                            + "ii = mergeEnd - 1;\n"
                            + "\n"
                            + "nodataleft: while (ii >= mergeStartPosition) {\n"
                            + "    int run1wins = 0;\n"
                            + "    int run2wins = 0;\n"
                            + "\n"
                            + "    if (context.minGallop < 2) {\n"
                            + "        context.minGallop = 2;\n"
                            + "    }\n"
                            + "\n"
                            + "    while (run1wins < context.minGallop && run2wins < context.minGallop) {\n"
                            + "        if (" + compareCall("val2Pos", "val1Pos") + " >= 0) {\n"
                            + "            positions.set(ii--, val2Pos);\n"
                            + "\n"
                            + "            if (--tempCursor < 0) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2Pos = context.temporaryPositions.get(tempCursor);\n"
                            + "\n"
                            + "            run2wins++;\n"
                            + "            run1wins = 0;\n"
                            + "        } else {\n"
                            + "            positions.set(ii--, val1Pos);\n"
                            + "\n"
                            + "            if (--run1Cursor < mergeStartPosition) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1Pos = positions.get(run1Cursor);\n"
                            + "\n"
                            + "            run1wins++;\n"
                            + "            run2wins = 0;\n"
                            + "        }\n"
                            + "    }\n"
                            + "\n"
                            + "    // we are in galloping mode now, if we had run out of data then we should have already bailed out to\n"
                            + "    // nodataleft\n"
                            + "    while (ii >= mergeStartPosition) {\n"
                            + "        // if we had a lot of things from run2, we take the next thing from run1 then find it in run2\n"
                            + "        final int copyUntil2 = lowerBound(context.temporaryPositions, " + valuesArgs()
                            + ", 0, tempCursor, val1Pos) + 1;\n"
                            + "\n"
                            + "        final int gallopLength2 = tempCursor - copyUntil2 + 1;\n"
                            + "        if (gallopLength2 > 0) {\n"
                            + "            copyToChunk(context.temporaryPositions, positions, copyUntil2, ii - gallopLength2 + 1, gallopLength2);\n"
                            + "            tempCursor -= gallopLength2;\n"
                            + "            ii -= gallopLength2;\n"
                            + "\n"
                            + "            if (tempCursor < 0) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val2Pos = context.temporaryPositions.get(tempCursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        // if we had a lot of things from run1, we take the next thing from run2 and then find it in run1\n"
                            + "        final int copyUntil1 = upperBound(positions, " + valuesArgs()
                            + ", mergeStartPosition, run1Cursor, val2Pos);\n"
                            + "\n"
                            + "        final int gallopLength1 = run1Cursor - copyUntil1;\n"
                            + "        if (gallopLength1 > 0) {\n"
                            + "            copyToChunk(positions, positions, copyUntil1, ii - gallopLength1, gallopLength1 + 1);\n"
                            + "            run1Cursor -= gallopLength1;\n"
                            + "            ii -= gallopLength1;\n"
                            + "\n"
                            + "            if (run1Cursor < mergeStartPosition) {\n"
                            + "                break nodataleft;\n"
                            + "            }\n"
                            + "            val1Pos = positions.get(run1Cursor);\n"
                            + "\n"
                            + "            context.minGallop--;\n"
                            + "        }\n"
                            + "\n"
                            + "        if (gallopLength1 < $utils:T.INITIAL_GALLOP && gallopLength2 < $utils:T.INITIAL_GALLOP) {\n"
                            + "            context.minGallop += 2; // undo the possible subtraction from above\n"
                            + "            break;\n"
                            + "        }\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "while (tempCursor >= 0) {\n"
                            + "    positions.set(ii, context.temporaryPositions.get(tempCursor));\n"
                            + "    tempCursor--;\n"
                            + "    ii--;\n"
                            + "}\n", Map.of("utils", TIMSORT_UTILS)).build())
                    .build();
        }

        private MethodSpec emitCopyToTemporary() {
            return MethodSpec.methodBuilder("copyToTemporary")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addParameter(wildcardContext, "context")
                    .addParameter(readablePositions, "positions")
                    .addParameter(int.class, "mergeStartPosition")
                    .addParameter(int.class, "remaining1")
                    .addStatement("context.temporaryPositions.setSize(remaining1)")
                    .addStatement("context.temporaryPositions.copyFromChunk(positions, mergeStartPosition, 0, "
                            + "remaining1)")
                    .build();
        }

        private MethodSpec emitCopyToChunk() {
            return MethodSpec.methodBuilder("copyToChunk")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addParameter(readablePositions, "positionsSource")
                    .addParameter(writablePositions, "positionsDest")
                    .addParameter(int.class, "sourceStart")
                    .addParameter(int.class, "destStart")
                    .addParameter(int.class, "length")
                    .addStatement("positionsDest.copyFromChunk(positionsSource, sourceStart, destStart, length)")
                    .build();
        }

        private MethodSpec.Builder boundMethod(final String name) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class)
                    .addParameter(readablePositions, "positions");
            addValuesParams(builder);
            return builder
                    .addParameter(int.class, "lo")
                    .addParameter(int.class, "hi")
                    .addParameter(int.class, "searchPos");
        }

        private MethodSpec emitUpperBound() {
            return boundMethod("upperBound")
                    .addComment(
                            "when we binary search in 1, we must identify a position for search value that is *after* our test values;")
                    .addComment("because the values from run 2 may never be inserted before an equal value from run 1")
                    .addComment("")
                    .addComment("lo is inclusive, hi is exclusive")
                    .addComment("")
                    .addComment(
                            "returns the position of the first element that is > searchValue or hi if there is no such element")
                    .addStatement("return bound(positions, " + valuesArgs() + ", lo, hi, searchPos, false)")
                    .build();
        }

        private MethodSpec emitLowerBound() {
            return boundMethod("lowerBound")
                    .addComment(
                            "when we binary search in 2, we must identify a position for search value that is *before* our test values;")
                    .addComment("because the values from run 1 may never be inserted after an equal value from run 2")
                    .addStatement("return bound(positions, " + valuesArgs() + ", lo, hi, searchPos, true)")
                    .build();
        }

        private MethodSpec emitBound() {
            return boundMethod("bound")
                    .addParameter(ParameterSpec.builder(boolean.class, "lower", Modifier.FINAL).build())
                    .addCode(CodeBlock.builder().add(""
                            + "final int compareLimit = lower ? -1 : 0; // lt or leq\n"
                            + "\n"
                            + "while (lo < hi) {\n"
                            + "    final int mid = (lo + hi) >>> 1;\n"
                            + "    final boolean moveLo = " + compareCall("positions.get(mid)", "searchPos")
                            + " <= compareLimit;\n"
                            + "    if (moveLo) {\n"
                            + "        // For bound, (testValue OP searchValue) means that the result somewhere later than 'mid' [OP=lt or leq]\n"
                            + "        lo = mid + 1;\n"
                            + "    } else {\n"
                            + "        hi = mid;\n"
                            + "    }\n"
                            + "}\n"
                            + "\n"
                            + "return lo;\n").build())
                    .build();
        }

        private MethodSpec emitInsertionSort() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("insertionSort")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addParameter(writablePositions, "positions");
            addValuesParams(builder);
            return builder
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addCode(CodeBlock.builder().add(""
                            + "for (int ii = offset + 1; ii < offset + length; ++ii) {\n"
                            + "    for (int jj = ii; jj > offset && "
                            + compareCall("positions.get(jj - 1)", "positions.get(jj)") + " > 0; jj--) {\n"
                            + "        swap(positions, jj, jj - 1);\n"
                            + "    }\n"
                            + "}\n").build())
                    .build();
        }

        private MethodSpec emitSwap() {
            return MethodSpec.methodBuilder("swap")
                    .addModifiers(Modifier.STATIC, Modifier.PRIVATE)
                    .addParameter(writablePositions, "positions")
                    .addParameter(int.class, "a")
                    .addParameter(int.class, "b")
                    .addCode(CodeBlock.builder().add(""
                            + "final int tempPos = positions.get(a);\n"
                            + "positions.set(a, positions.get(b));\n"
                            + "positions.set(b, tempPos);\n").build())
                    .build();
        }
    }
}
