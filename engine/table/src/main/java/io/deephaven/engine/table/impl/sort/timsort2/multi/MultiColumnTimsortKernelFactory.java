//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort2.multi;

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
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import org.jetbrains.annotations.Nullable;

import javax.lang.model.element.Modifier;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Provides indirect timsort kernels for sort keys made up of one or more columns: the kernels permute a parallel chunk
 * of int positions (reading the never-moved column values through the positions for each lazy per-column comparison)
 * and assemble the permuted row keys in a single linear pass at the end.
 *
 * <p>
 * Kernels for the common shapes — a single Object column in either direction, and every ascending pair of column types
 * — are pregenerated into this package (via {@code GenerateTimsortKernels}) and selected by
 * {@code IndirectMultiColumnTimsortDispatcher}. Other multi-column shapes (three or more columns, or pairs with a
 * descending column) are generated with JavaPoet and compiled on demand, as the typed aggregation hashers are; the
 * compiled kernels are cached by class name.
 *
 * <p>
 * {@link #makeContext} is also the policy authority: single-column sorts of primitive types return null, as the
 * existing direct (value-moving) kernels are faster for them; callers fall back to those kernels.
 */
public class MultiColumnTimsortKernelFactory {
    private static final String PACKAGE = "io.deephaven.engine.table.impl.sort.timsort2.multi";
    private static final String RUNTIME_PACKAGE_ROOT = PACKAGE + ".gen";

    private static final ClassName ANY = ClassName.get("io.deephaven.chunk.attributes", "Any");
    private static final ClassName CHUNK_POSITIONS = ClassName.get("io.deephaven.chunk.attributes", "ChunkPositions");
    private static final ClassName INT_CHUNK = ClassName.get("io.deephaven.chunk", "IntChunk");
    private static final ClassName WRITABLE_INT_CHUNK = ClassName.get("io.deephaven.chunk", "WritableIntChunk");
    private static final ClassName WRITABLE_LONG_CHUNK = ClassName.get("io.deephaven.chunk", "WritableLongChunk");
    private static final ClassName WRITABLE_CHUNK = ClassName.get("io.deephaven.chunk", "WritableChunk");
    private static final ClassName MULTI_COLUMN_SORT_KERNEL =
            ClassName.get("io.deephaven.engine.table.impl.sort", "MultiColumnSortKernel");
    private static final ClassName TIMSORT_UTILS =
            ClassName.get("io.deephaven.engine.table.impl.sort.timsort", "TimsortUtils");

    /** Cached createContext methods of runtime-compiled kernels, keyed by kernel class name. */
    private static final Map<String, Method> COMPILED_KERNELS = new ConcurrentHashMap<>();

    /** The element type, chunk classes, and comparison for one sort key column of a given chunk type. */
    enum ColumnType {
        // chars sort with Deephaven null-aware semantics, matching LongSortKernel.makeContext
        NULL_AWARE_CHAR("NullAwareChar", "Char", TypeName.CHAR,
                ClassName.get("io.deephaven.util.compare", "CharComparisons")), BYTE("Byte", "Byte", TypeName.BYTE,
                        ClassName.get(Byte.class)), SHORT("Short", "Short", TypeName.SHORT, ClassName
                                .get(Short.class)), INT("Int", "Int", TypeName.INT, ClassName.get(Integer.class)), LONG(
                                        "Long", "Long", TypeName.LONG, ClassName.get(Long.class)), FLOAT("Float",
                                                "Float", TypeName.FLOAT,
                                                ClassName.get("io.deephaven.util.compare", "FloatComparisons")), DOUBLE(
                                                        "Double", "Double", TypeName.DOUBLE,
                                                        ClassName.get("io.deephaven.util.compare",
                                                                "DoubleComparisons")), OBJECT("Object", "Object",
                                                                        ClassName.OBJECT,
                                                                        ClassName.get("io.deephaven.util.compare",
                                                                                "ObjectComparisons"));

        final String namePart;
        final ClassName chunkName;
        final TypeName elementType;
        final ClassName compareClass;
        final boolean isObject;

        ColumnType(final String namePart, final String chunkFamily, final TypeName elementType,
                final ClassName compareClass) {
            this.namePart = namePart;
            this.chunkName = ClassName.get("io.deephaven.chunk", chunkFamily + "Chunk");
            this.elementType = elementType;
            this.compareClass = compareClass;
            this.isObject = !elementType.isPrimitive();
        }

        static @Nullable ColumnType forChunkType(final ChunkType chunkType) {
            switch (chunkType) {
                case Char:
                    return NULL_AWARE_CHAR;
                case Byte:
                    return BYTE;
                case Short:
                    return SHORT;
                case Int:
                    return INT;
                case Long:
                    return LONG;
                case Float:
                    return FLOAT;
                case Double:
                    return DOUBLE;
                case Object:
                    return OBJECT;
                default:
                    // boolean columns are reinterpreted to bytes before sorting; no kernel for raw boolean chunks
                    return null;
            }
        }

        TypeName chunkOfWildcard() {
            final WildcardTypeName wildcard = WildcardTypeName.subtypeOf(ClassName.OBJECT);
            return isObject
                    ? ParameterizedTypeName.get(chunkName, ClassName.OBJECT, wildcard)
                    : ParameterizedTypeName.get(chunkName, wildcard);
        }

        String asChunkMethod() {
            return "as" + chunkName.simpleName();
        }

        /** The body of this column's {@code doComparison}, in terms of parameters {@code lhs} and {@code rhs}. */
        CodeBlock comparisonExpression(final SortingOrder order) {
            if (this == OBJECT) {
                // descending Object comparison swaps the arguments rather than negating
                return order == SortingOrder.Ascending
                        ? CodeBlock.of("$T.compare(lhs, rhs)", compareClass)
                        : CodeBlock.of("$T.compare(rhs, lhs)", compareClass);
            }
            return order == SortingOrder.Ascending
                    ? CodeBlock.of("$T.compare(lhs, rhs)", compareClass)
                    : CodeBlock.of("-1 * $T.compare(lhs, rhs)", compareClass);
        }
    }

    /**
     * Returns an indirect sort kernel context for the given column chunk types and sort directions, or null when the
     * existing single-column kernels should be used instead: single-column sorts of primitive types (where the direct,
     * value-moving kernels are faster), and any sort involving a boolean chunk.
     */
    public static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeContext(
            final ChunkType[] chunkTypes, final SortingOrder[] order, final int size) {
        if (chunkTypes.length == 1 && chunkTypes[0] != ChunkType.Object) {
            // the direct single-column kernels beat indirection for primitive columns
            return null;
        }
        for (final ChunkType chunkType : chunkTypes) {
            if (ColumnType.forChunkType(chunkType) == null) {
                return null;
            }
        }
        final MultiColumnSortKernel<PERMUTE_VALUES_ATTR> pregenerated =
                IndirectMultiColumnTimsortDispatcher.makeContext(chunkTypes, order, size);
        if (pregenerated != null) {
            return pregenerated;
        }
        return makeCompiledContext(chunkTypes, order, size);
    }

    private static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeCompiledContext(
            final ChunkType[] chunkTypes, final SortingOrder[] order, final int size) {
        final String className = kernelName(chunkTypes, order);
        final Method createContext = COMPILED_KERNELS.computeIfAbsent(className, name -> {
            final JavaFile javaFile =
                    JavaFile.builder(PACKAGE, generateKernelType(chunkTypes, order, PACKAGE)).indent("    ").build();
            final String classBody = Arrays.stream(javaFile.toString().split("\n"))
                    .filter(s -> !s.startsWith("package "))
                    .collect(Collectors.joining("\n"));
            final Class<?> clazz = ExecutionContext.getContext().getQueryCompiler()
                    .compile(QueryCompilerRequest.builder()
                            .description("MultiColumnTimsortKernelFactory: " + name)
                            .className(name)
                            .classBody(classBody)
                            .packageNameRoot(RUNTIME_PACKAGE_ROOT)
                            .build());
            try {
                return clazz.getDeclaredMethod("createContext", int.class);
            } catch (NoSuchMethodException e) {
                throw new UncheckedDeephavenException("Compiled kernel " + name + " has no createContext method", e);
            }
        });
        try {
            // noinspection unchecked
            return (MultiColumnSortKernel<PERMUTE_VALUES_ATTR>) createContext.invoke(null, size);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new UncheckedDeephavenException("Could not create compiled sort kernel context " + className, e);
        }
    }

    /**
     * The kernel class name for the given column chunk types and sort directions; descending columns append
     * {@code Desc} to their name part.
     */
    public static String kernelName(final ChunkType[] chunkTypes, final SortingOrder[] order) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.append(ColumnType.forChunkType(chunkTypes[ii]).namePart);
            if (order[ii] == SortingOrder.Descending) {
                builder.append("Desc");
            }
        }
        builder.append(chunkTypes.length > 1 ? "IndirectMultiColumnTimsortKernel" : "IndirectTimsortKernel");
        return builder.toString();
    }

    /**
     * Generates the kernel class for the given column chunk types and sort directions; used both by the replicator to
     * pregenerate the common shapes and at runtime to compile the rest on demand.
     */
    public static TypeSpec generateKernelType(final ChunkType[] chunkTypes, final SortingOrder[] order,
            final String packageName) {
        return new KernelEmitter(chunkTypes, order, packageName).emit();
    }

    private static final String MERGE_JAVADOC_FRONT =
            "Merge context temporary and run2 between mergeStartPosition and length2 (which is not the full run "
                    + "length, but\nthe length of things we might need to merge.\n"
                    + "<p>\nWe eventually need to do galloping here, but are skipping that for now\n";

    private static final String MERGE_JAVADOC_BACK =
            "Merge context temporary and run1 between mergeStartPosition + length1 + temporary.length\n"
                    + "<p>\nWe eventually need to do galloping here, but are skipping that for now\n";

    /**
     * Emits an indirect kernel: the timsort permutes only a parallel chunk of int positions, reading the (never moved)
     * column values through those positions for each lazy per-column comparison. The context's array-based sort bridge
     * initializes the identity positions, runs the sort, and then assembles the permuted row keys in a single linear
     * pass — the row keys are not permuted during the sort either.
     */
    static final class KernelEmitter {
        private final ChunkType[] chunkTypes;
        private final SortingOrder[] order;
        private final int n;

        private final TypeVariableName permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);
        private final ClassName kernelClass;
        private final ClassName contextClass;
        private final TypeName wildcardContext;
        private final TypeName writablePositions;
        private final TypeName readablePositions;

        KernelEmitter(final ChunkType[] chunkTypes, final SortingOrder[] order, final String packageName) {
            this.chunkTypes = chunkTypes;
            this.order = order;
            this.n = chunkTypes.length;
            this.kernelClass = ClassName.get(packageName, kernelName(chunkTypes, order));
            this.contextClass = kernelClass.nestedClass(
                    kernelName(chunkTypes, order).replace("TimsortKernel", "SortKernelContext"));
            this.wildcardContext =
                    ParameterizedTypeName.get(contextClass, WildcardTypeName.subtypeOf(ClassName.OBJECT));
            this.writablePositions = ParameterizedTypeName.get(WRITABLE_INT_CHUNK, CHUNK_POSITIONS);
            this.readablePositions = ParameterizedTypeName.get(INT_CHUNK, CHUNK_POSITIONS);
        }

        private ColumnType column(final int k) {
            return ColumnType.forChunkType(chunkTypes[k]);
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
                builder.addParameter(ParameterSpec.builder(column(k).chunkOfWildcard(), "valuesToSort" + k).build());
            }
        }

        TypeSpec emit() {
            final StringBuilder keyDescription = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    keyDescription.append(", ");
                }
                keyDescription.append(column(k).namePart);
                if (order[k] == SortingOrder.Descending) {
                    keyDescription.append(" descending");
                }
            }
            final TypeSpec.Builder builder = TypeSpec.classBuilder(kernelClass)
                    .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
                    .addJavadoc("This implements a timsort kernel for a sort key ($L) that never moves the "
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
            final TypeSpec.Builder context = TypeSpec.classBuilder(contextClass.simpleName())
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
                    .builder(ParameterizedTypeName.get(WRITABLE_LONG_CHUNK, permuteAttr), "temporaryKeys",
                            Modifier.PRIVATE, Modifier.FINAL)
                    .build());

            context.addMethod(MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(int.class, "size")
                    .addStatement("positions = $T.makeWritableChunk(size)", WRITABLE_INT_CHUNK)
                    .addStatement("temporaryPositions = $T.makeWritableChunk((size + 2) / 2)", WRITABLE_INT_CHUNK)
                    .addStatement("temporaryKeys = $T.makeWritableChunk(size)", WRITABLE_LONG_CHUNK)
                    .addStatement("runStarts = new int[(size + 31) / 32]")
                    .addStatement("runLengths = new int[(size + 31) / 32]")
                    .addStatement("minGallop = $T.INITIAL_GALLOP", TIMSORT_UTILS)
                    .build());

            final StringBuilder sortFormat = new StringBuilder("$T.sort(this, positions");
            final List<Object> sortArgs = new ArrayList<>();
            sortArgs.add(kernelClass);
            for (int k = 0; k < n; ++k) {
                if (column(k).isObject) {
                    sortFormat.append(", valuesToSort[").append(k).append("].<$T>").append(column(k).asChunkMethod())
                            .append("()");
                    sortArgs.add(ClassName.OBJECT);
                } else {
                    sortFormat.append(", valuesToSort[").append(k).append("].").append(column(k).asChunkMethod())
                            .append("()");
                }
            }
            sortFormat.append(")");
            context.addMethod(MethodSpec.methodBuilder("sort")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(ParameterizedTypeName.get(WRITABLE_LONG_CHUNK, permuteAttr), "valuesToPermute")
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
                    .addJavadoc("Sort the positions chunk such that the values it points to are ordered by this "
                            + "kernel's sort key,\ncomparing each column in turn; the value chunks themselves are "
                            + "not modified.\n")
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
                    .addParameter(column(k).elementType, "lhs")
                    .addParameter(column(k).elementType, "rhs")
                    .addStatement("return $L", column(k).comparisonExpression(order[k]))
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
                builder.beginControlFlow("if (cmp$L != 0)", k - 1);
                builder.addStatement("return cmp$L", k - 1);
                builder.endControlFlow();
                if (k < n - 1) {
                    builder.addStatement(
                            "final int cmp$L = doComparison$L(valuesToSort$L.get(lhsPos), valuesToSort$L.get(rhsPos))",
                            k, k, k, k);
                } else {
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
                    .addJavadoc(MERGE_JAVADOC_FRONT)
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
                    .addJavadoc(MERGE_JAVADOC_BACK)
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
