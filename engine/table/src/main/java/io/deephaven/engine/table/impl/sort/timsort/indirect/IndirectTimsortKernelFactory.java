//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort.indirect;

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
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.compare.DoubleComparisons;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.util.compare.ObjectComparisons;
import org.jetbrains.annotations.Nullable;

import javax.lang.model.element.Modifier;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
 * The single-column kernels — including the comparator kernel — are pregenerated into this package (via
 * {@code GenerateTimsortKernels}) and selected by {@code IndirectTimsortDispatcher}. Every multi-column shape — any mix
 * of column types, sort directions, and per-column comparators — is generated with JavaPoet and compiled on demand, as
 * the typed aggregation hashers are; the compiled kernels are cached by class name.
 *
 * <p>
 * {@link #makeContext} is also the policy authority: single-column sorts of primitive types return null, as the
 * existing direct (value-moving) kernels are faster for them; callers fall back to those kernels.
 */
public class IndirectTimsortKernelFactory {
    private static final String PACKAGE = "io.deephaven.engine.table.impl.sort.timsort.indirect";
    private static final String RUNTIME_PACKAGE_ROOT = PACKAGE + ".gen";

    private static final ClassName ANY = ClassName.get(Any.class);
    private static final ClassName CHUNK_POSITIONS = ClassName.get(ChunkPositions.class);
    private static final ClassName INT_CHUNK = ClassName.get(IntChunk.class);
    private static final ClassName WRITABLE_INT_CHUNK = ClassName.get(WritableIntChunk.class);
    private static final ClassName WRITABLE_LONG_CHUNK = ClassName.get(WritableLongChunk.class);
    private static final ClassName WRITABLE_CHUNK = ClassName.get(WritableChunk.class);
    private static final ClassName MULTI_COLUMN_SORT_KERNEL = ClassName.get(MultiColumnSortKernel.class);
    private static final ClassName TIMSORT_UTILS = ClassName.get(TimsortUtils.class);

    /** Cached createContext methods of runtime-compiled kernels, keyed by kernel class name. */
    private static final Map<String, Method> COMPILED_KERNELS = new ConcurrentHashMap<>();

    /** The element type, chunk classes, and comparison for one sort key column of a given chunk type. */
    enum ColumnType {
        // @formatter:off
        // chars sort with Deephaven null-aware semantics, matching LongSortKernel.makeContext
        NULL_AWARE_CHAR("NullAwareChar", CharChunk.class, TypeName.CHAR, CharComparisons.class),
        BYTE("Byte", ByteChunk.class, TypeName.BYTE, Byte.class),
        SHORT("Short", ShortChunk.class, TypeName.SHORT, Short.class),
        INT("Int", IntChunk.class, TypeName.INT, Integer.class),
        LONG("Long", LongChunk.class, TypeName.LONG, Long.class),
        FLOAT("Float", FloatChunk.class, TypeName.FLOAT, FloatComparisons.class),
        DOUBLE("Double", DoubleChunk.class, TypeName.DOUBLE, DoubleComparisons.class),
        OBJECT("Object", ObjectChunk.class, ClassName.OBJECT, ObjectComparisons.class);
        // @formatter:on

        final String namePart;
        final ClassName chunkName;
        final TypeName elementType;
        final ClassName compareClass;
        final boolean isObject;

        ColumnType(final String namePart, final Class<?> chunkClass, final TypeName elementType,
                final Class<?> compareClass) {
            this.namePart = namePart;
            this.chunkName = ClassName.get(chunkClass);
            this.elementType = elementType;
            this.compareClass = ClassName.get(compareClass);
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
     * Returns an indirect sort kernel context for the given column chunk types, sort directions, and per-column
     * comparators (each entry null for a column ordered by its natural Deephaven ordering), or null when the existing
     * single-column kernels should be used instead: single-column sorts of primitive types (where the direct,
     * value-moving kernels are faster) and any sort involving a boolean chunk.
     *
     * <p>
     * Multi-column sorts are always handled: the single-column indirect kernels are pregenerated, and every
     * multi-column shape — any mix of column types, directions, and comparators — is compiled on demand and cached.
     * Comparator columns compare with the supplied comparator (reversed for descending columns); ties under the
     * comparator are broken by the subsequent sort columns.
     */
    public static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeContext(
            final ChunkType[] chunkTypes, final SortingOrder[] order, final Comparator[] comparators, final int size) {
        if (!hasKernel(chunkTypes, comparators)) {
            return null;
        }
        if (chunkTypes.length == 1) {
            return IndirectTimsortDispatcher.makeContext(chunkTypes, order, comparators, size);
        }
        return makeCompiledContext(chunkTypes, order, comparators, size);
    }

    /**
     * Whether {@link #makeContext} provides a kernel for the given column chunk types and comparators; callers use the
     * existing single-column kernels when it does not. This is the same policy makeContext applies, without creating a
     * context: single-column sorts of primitive types are declined, as are boolean chunks.
     */
    public static boolean hasKernel(final ChunkType[] chunkTypes, final Comparator[] comparators) {
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            if (ColumnType.forChunkType(chunkTypes[ii]) == null) {
                return false;
            }
            Assert.assertion(comparators[ii] == null || chunkTypes[ii] == ChunkType.Object,
                    "comparators[ii] == null || chunkTypes[ii] == ChunkType.Object");
        }
        if (chunkTypes.length == 1) {
            // the direct single-column kernels beat indirection for primitive columns
            return chunkTypes[0] == ChunkType.Object;
        }
        return true;
    }

    /**
     * Resolve — compiling on demand if necessary — the kernel that {@link #makeContext} would use for the given shape,
     * without creating a context. Sort operations call this when they are constructed, on a thread whose
     * ExecutionContext has a QueryCompiler, so that later sorts on update graph threads (whose contexts cannot compile)
     * find the kernel already cached.
     */
    public static void prepareKernel(final ChunkType[] chunkTypes, final SortingOrder[] order,
            final Comparator[] comparators) {
        if (!hasKernel(chunkTypes, comparators) || chunkTypes.length == 1) {
            // unsupported shapes use the single-column kernels, and the single-column indirect kernels — including
            // the comparator kernel — are pregenerated
            return;
        }
        ensureCompiled(chunkTypes, order, comparators);
    }

    private static <PERMUTE_VALUES_ATTR extends Any> MultiColumnSortKernel<PERMUTE_VALUES_ATTR> makeCompiledContext(
            final ChunkType[] chunkTypes, final SortingOrder[] order, final Comparator[] comparators, final int size) {
        final Method createContext = ensureCompiled(chunkTypes, order, comparators);
        boolean anyComparator = false;
        for (int ii = 0; ii < comparators.length; ++ii) {
            anyComparator |= comparators[ii] != null;
        }
        try {
            if (!anyComparator) {
                // noinspection unchecked
                return (MultiColumnSortKernel<PERMUTE_VALUES_ATTR>) createContext.invoke(null, size);
            }
            // the generated comparison is always in the ascending sense for comparator columns; descending
            // comparator columns are handled by reversing the comparator
            final Comparator[] adjustedComparators = new Comparator[comparators.length];
            for (int ii = 0; ii < comparators.length; ++ii) {
                if (comparators[ii] != null) {
                    adjustedComparators[ii] =
                            order[ii] == SortingOrder.Ascending ? comparators[ii] : comparators[ii].reversed();
                }
            }
            // noinspection unchecked
            return (MultiColumnSortKernel<PERMUTE_VALUES_ATTR>) createContext.invoke(null, size,
                    (Object) adjustedComparators);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new UncheckedDeephavenException(
                    "Could not create compiled sort kernel context " + createContext.getDeclaringClass().getName(), e);
        }
    }

    private static Method ensureCompiled(
            final ChunkType[] chunkTypes, final SortingOrder[] order, final Comparator[] comparators) {
        final boolean[] hasComparator = new boolean[comparators.length];
        boolean foundComparator = false;
        for (int ii = 0; ii < comparators.length; ++ii) {
            hasComparator[ii] = comparators[ii] != null;
            foundComparator |= hasComparator[ii];
        }
        final boolean anyComparator = foundComparator;

        final String className = kernelName(chunkTypes, order, hasComparator);
        return COMPILED_KERNELS.computeIfAbsent(className, name -> {
            final JavaFile javaFile = JavaFile
                    .builder(PACKAGE, generateKernelType(chunkTypes, order, hasComparator, PACKAGE)).indent("    ")
                    .build();
            final String classBody = Arrays.stream(javaFile.toString().split("\n"))
                    .filter(s -> !s.startsWith("package "))
                    .collect(Collectors.joining("\n"));
            final Class<?> clazz = ExecutionContext.getContext().getQueryCompiler()
                    .compile(QueryCompilerRequest.builder()
                            .description("IndirectTimsortKernelFactory: " + name)
                            .className(name)
                            .classBody(classBody)
                            .packageNameRoot(RUNTIME_PACKAGE_ROOT)
                            .build());
            try {
                return anyComparator
                        ? clazz.getDeclaredMethod("createContext", int.class, Comparator[].class)
                        : clazz.getDeclaredMethod("createContext", int.class);
            } catch (NoSuchMethodException e) {
                throw new UncheckedDeephavenException("Compiled kernel " + name + " has no createContext method", e);
            }
        });
    }

    /**
     * The kernel class name for the given column chunk types, sort directions, and comparator columns; descending
     * columns append {@code Desc} to their name part, and comparator columns use the name part {@code Comparator}
     * (their direction is handled by reversing the comparator, so it is not part of the name).
     */
    public static String kernelName(final ChunkType[] chunkTypes, final SortingOrder[] order,
            final boolean[] hasComparator) {
        final StringBuilder builder = new StringBuilder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            if (hasComparator[ii]) {
                builder.append("Comparator");
                continue;
            }
            builder.append(ColumnType.forChunkType(chunkTypes[ii]).namePart);
            if (order[ii] == SortingOrder.Descending) {
                builder.append("Desc");
            }
        }
        builder.append(chunkTypes.length > 1 ? "IndirectMultiColumnTimsortKernel" : "IndirectTimsortKernel");
        return builder.toString();
    }

    /**
     * Generates the kernel class for the given column chunk types, sort directions, and comparator columns; used both
     * by the replicator to pregenerate the single-column kernels and at runtime to compile the multi-column kernels on
     * demand.
     */
    public static TypeSpec generateKernelType(final ChunkType[] chunkTypes, final SortingOrder[] order,
            final boolean[] hasComparator, final String packageName) {
        return new KernelEmitter(chunkTypes, order, hasComparator, packageName).emit();
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
        private final boolean[] hasComparator;
        private final boolean anyComparator;
        private final int n;

        private final TypeVariableName permuteAttr = TypeVariableName.get("PERMUTE_VALUES_ATTR", ANY);
        private final ClassName kernelClass;
        private final ClassName contextClass;
        private final TypeName wildcardContext;
        private final TypeName writablePositions;
        private final TypeName readablePositions;

        KernelEmitter(final ChunkType[] chunkTypes, final SortingOrder[] order, final boolean[] hasComparator,
                final String packageName) {
            this.chunkTypes = chunkTypes;
            this.order = order;
            this.hasComparator = hasComparator;
            boolean any = false;
            for (final boolean columnHasComparator : hasComparator) {
                any |= columnHasComparator;
            }
            this.anyComparator = any;
            this.n = chunkTypes.length;
            final String name = kernelName(chunkTypes, order, hasComparator);
            this.kernelClass = ClassName.get(packageName, name);
            this.contextClass = kernelClass.nestedClass(name.replace("TimsortKernel", "SortKernelContext"));
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

        /**
         * The leading context argument for the comparison methods; comparator kernels read their comparators from the
         * context, so the comparisons (and the methods that only call them) need it.
         */
        private String contextArg() {
            return anyComparator ? "context, " : "";
        }

        /** A lazy per-column comparison of the elements at two positions. */
        private String compareCall(final String lhsPos, final String rhsPos) {
            return "compareColumns(" + contextArg() + valuesArgs() + ", " + lhsPos + ", " + rhsPos + ")";
        }

        /** A named comparison predicate (gt, lt, geq, leq) applied to the elements at two positions. */
        private String opCall(final String op, final String lhsPos, final String rhsPos) {
            return op + "(" + contextArg() + valuesArgs() + ", " + lhsPos + ", " + rhsPos + ")";
        }

        /** Adds the context parameter used by the comparison methods of comparator kernels. */
        private void maybeAddContextParam(final MethodSpec.Builder builder) {
            if (anyComparator) {
                builder.addParameter(wildcardContext, "context");
            }
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
            for (final String[] op : new String[][] {
                    {"gt", ">"}, {"lt", "<"}, {"geq", ">="}, {"leq", "<="}}) {
                builder.addMethod(emitCompareOp(op[0], op[1]));
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

        /** The arguments that unpack the values array into the kernel's typed chunk parameters. */
        private String valuesArrayArgs(final List<Object> formatArgs) {
            final StringBuilder builder = new StringBuilder();
            for (int k = 0; k < n; ++k) {
                if (k > 0) {
                    builder.append(", ");
                }
                if (column(k).isObject) {
                    builder.append("valuesToSort[").append(k).append("].<$T>").append(column(k).asChunkMethod())
                            .append("()");
                    formatArgs.add(ClassName.OBJECT);
                } else {
                    builder.append("valuesToSort[").append(k).append("].").append(column(k).asChunkMethod())
                            .append("()");
                }
            }
            return builder.toString();
        }

        private static final TypeName VALUES_ARRAY =
                ArrayTypeName.of(ParameterizedTypeName.get(WRITABLE_CHUNK, WildcardTypeName.subtypeOf(ANY)));

        private TypeSpec emitContext() {
            final TypeSpec.Builder context = TypeSpec.classBuilder(contextClass.simpleName())
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(permuteAttr)
                    .addSuperinterface(ParameterizedTypeName.get(MULTI_COLUMN_SORT_KERNEL, permuteAttr));

            context.addField(int.class, "minGallop");
            context.addField(FieldSpec.builder(int.class, "runCount").initializer("0").build());
            context.addField(FieldSpec.builder(int.class, "size", Modifier.PRIVATE, Modifier.FINAL).build());
            context.addField(int[].class, "runStarts", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(int[].class, "runLengths", Modifier.PRIVATE, Modifier.FINAL);
            context.addField(
                    FieldSpec.builder(writablePositions, "temporaryPositions", Modifier.PRIVATE, Modifier.FINAL)
                            .build());
            // the identity positions and the row-key gather buffer are only used by the whole-chunk sort bridge;
            // contexts used as scratch by the parallel sort driver never allocate them
            context.addField(FieldSpec.builder(writablePositions, "positions", Modifier.PRIVATE).build());
            context.addField(FieldSpec
                    .builder(ParameterizedTypeName.get(WRITABLE_LONG_CHUNK, permuteAttr), "temporaryKeys",
                            Modifier.PRIVATE)
                    .build());

            for (int k = 0; k < n; ++k) {
                if (hasComparator[k]) {
                    context.addField(FieldSpec
                            .builder(ClassName.get(Comparator.class), "comparator" + k, Modifier.FINAL)
                            .build());
                }
            }

            final MethodSpec.Builder constructor = MethodSpec.constructorBuilder()
                    .addModifiers(Modifier.PRIVATE)
                    .addParameter(int.class, "size");
            if (anyComparator) {
                constructor.addParameter(ArrayTypeName.of(ClassName.get(Comparator.class)), "comparators");
                for (int k = 0; k < n; ++k) {
                    if (hasComparator[k]) {
                        constructor.addStatement("comparator$L = comparators[$L]", k, k);
                    }
                }
            }
            constructor
                    .addStatement("this.size = size")
                    .addStatement("temporaryPositions = $T.makeWritableChunk((size + 2) / 2)", WRITABLE_INT_CHUNK)
                    .addStatement("runStarts = new int[(size + 31) / 32]")
                    .addStatement("runLengths = new int[(size + 31) / 32]")
                    .addStatement("minGallop = $T.INITIAL_GALLOP", TIMSORT_UTILS);
            context.addMethod(constructor.build());

            final List<Object> sortArgs = new ArrayList<>();
            sortArgs.add(kernelClass);
            final String sortFormat = "$T.sort(this, positions, " + valuesArrayArgs(sortArgs) + ")";
            context.addMethod(MethodSpec.methodBuilder("sort")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(ParameterizedTypeName.get(WRITABLE_LONG_CHUNK, permuteAttr), "valuesToPermute")
                    .addParameter(VALUES_ARRAY, "valuesToSort")
                    .addStatement("final int sortSize = valuesToPermute.size()")
                    .beginControlFlow("if (positions == null)")
                    .addStatement("positions = $T.makeWritableChunk(size)", WRITABLE_INT_CHUNK)
                    .addStatement("temporaryKeys = $T.makeWritableChunk(size)", WRITABLE_LONG_CHUNK)
                    .endControlFlow()
                    .addStatement("positions.setSize(sortSize)")
                    .beginControlFlow("for (int ii = 0; ii < sortSize; ++ii)")
                    .addStatement("positions.set(ii, ii)")
                    .endControlFlow()
                    .addStatement(sortFormat, sortArgs.toArray())
                    .addComment(
                            "assemble the permuted row keys in a single linear pass rather than permuting them during the sort")
                    .addStatement("temporaryKeys.copyFromChunk(valuesToPermute, 0, 0, sortSize)")
                    .beginControlFlow("for (int ii = 0; ii < sortSize; ++ii)")
                    .addStatement("valuesToPermute.set(ii, temporaryKeys.get(positions.get(ii)))")
                    .endControlFlow()
                    .build());

            final List<Object> sortPositionsArgs = new ArrayList<>();
            sortPositionsArgs.add(kernelClass);
            final String sortPositionsFormat =
                    "$T.timSort(this, positions, " + valuesArrayArgs(sortPositionsArgs) + ", offset, length)";
            context.addMethod(MethodSpec.methodBuilder("sortPositions")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(writablePositions, "positions")
                    .addParameter(VALUES_ARRAY, "valuesToSort")
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .addStatement(sortPositionsFormat, sortPositionsArgs.toArray())
                    .build());

            final List<Object> mergePositionsArgs = new ArrayList<>();
            mergePositionsArgs.add(kernelClass);
            final String mergePositionsFormat =
                    "$T.merge(this, positions, " + valuesArrayArgs(mergePositionsArgs) + ", start1, length1, length2)";
            context.addMethod(MethodSpec.methodBuilder("mergePositions")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(writablePositions, "positions")
                    .addParameter(VALUES_ARRAY, "valuesToSort")
                    .addParameter(int.class, "start1")
                    .addParameter(int.class, "length1")
                    .addParameter(int.class, "length2")
                    .addStatement(mergePositionsFormat, mergePositionsArgs.toArray())
                    .build());

            final MethodSpec.Builder close = MethodSpec.methodBuilder("close")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addStatement("temporaryPositions.close()")
                    .beginControlFlow("if (positions != null)")
                    .addStatement("positions.close()")
                    .addStatement("temporaryKeys.close()")
                    .endControlFlow();
            context.addMethod(close.build());
            return context.build();
        }

        private MethodSpec emitCreateContext() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("createContext")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(permuteAttr)
                    .returns(ParameterizedTypeName.get(contextClass, permuteAttr))
                    .addParameter(ParameterSpec.builder(int.class, "size", Modifier.FINAL).build());
            if (anyComparator) {
                builder.addJavadoc("The comparators array has one entry per sort column, null for columns compared "
                        + "by their natural\nDeephaven ordering; descending comparator columns must supply an "
                        + "already-reversed comparator.\n")
                        .addParameter(ParameterSpec
                                .builder(ArrayTypeName.of(ClassName.get(Comparator.class)), "comparators",
                                        Modifier.FINAL)
                                .build())
                        .addStatement("return new $T<>(size, comparators)", contextClass);
            } else {
                builder.addStatement("return new $T<>(size)", contextClass);
            }
            return builder.build();
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
                    .beginControlFlow("if (length <= 1)")
                    .addStatement("return")
                    .endControlFlow()
                    .addStatement("final int minRun = $T.getRunLength(length)", TIMSORT_UTILS)
                    .beginControlFlow("if (length <= minRun)")
                    .addStatement("insertionSort(" + contextArg() + "positions, " + valuesArgs() + ", offset, length)")
                    .addStatement("return")
                    .endControlFlow()
                    .addStatement("context.runCount = 0")
                    .addStatement("int startRun = offset")
                    .beginControlFlow("while (startRun < offset + length)")
                    .addStatement("int currentPos = positions.get(startRun)")
                    .addComment("note that endrun is exclusive")
                    .addStatement("int endRun")
                    .addStatement("final boolean descending")
                    .beginControlFlow("if (startRun + 1 == offset + length)")
                    .addStatement("endRun = offset + length")
                    .addStatement("descending = false")
                    .nextControlFlow("else")
                    .addStatement("int nextPos = positions.get(startRun + 1)")
                    .addStatement("endRun = startRun + 2")
                    .addStatement("descending = " + opCall("gt", "currentPos", "nextPos"))
                    .beginControlFlow("if (!descending)")
                    .addComment("search for a non-descending run")
                    .addStatement("currentPos = nextPos")
                    .beginControlFlow("while (endRun < length && "
                            + opCall("geq", "nextPos = positions.get(endRun)", "currentPos") + ")")
                    .addStatement("currentPos = nextPos")
                    .addStatement("endRun++")
                    .endControlFlow()
                    .nextControlFlow("else")
                    .addComment(
                            "search for a strictly descending run; we can not have any equal values, or we will break the")
                    .addComment("sort's stability guarantee")
                    .addStatement("currentPos = nextPos")
                    .beginControlFlow("while (endRun < length && "
                            + opCall("lt", "nextPos = positions.get(endRun)", "currentPos") + ")")
                    .addStatement("currentPos = nextPos")
                    .addStatement("endRun++")
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow()
                    .addStatement("final int foundLength = endRun - startRun")
                    .addStatement("context.runStarts[context.runCount] = startRun")
                    .beginControlFlow("if (foundLength < minRun)")
                    .addComment("increase the size of the run to the minimum run")
                    .addStatement("final int actualLength = Math.min(minRun, length - (startRun - offset))")
                    .addStatement("insertionSort(" + contextArg() + "positions, " + valuesArgs()
                            + ", startRun, actualLength)")
                    .addStatement("context.runLengths[context.runCount] = actualLength")
                    .addStatement("startRun += actualLength")
                    .nextControlFlow("else")
                    .beginControlFlow("if (descending)")
                    .addComment("reverse the current run")
                    .beginControlFlow("for (int ii = 0; ii < foundLength / 2; ++ii)")
                    .addStatement("swap(positions, ii + startRun, endRun - ii - 1)")
                    .endControlFlow()
                    .endControlFlow()
                    .addComment("now an ascending run")
                    .addStatement("context.runLengths[context.runCount] = foundLength")
                    .addStatement("startRun = endRun")
                    .endControlFlow()
                    .addStatement("context.runCount++")
                    .addComment("check the invariants at the top of the stack")
                    .addStatement("ensureMergeInvariants(context, positions, " + valuesArgs() + ")")
                    .endControlFlow()
                    .beginControlFlow("while (context.runCount > 1)")
                    .addStatement("final int length2 = context.runLengths[context.runCount - 1]")
                    .addStatement("final int start1 = context.runStarts[context.runCount - 2]")
                    .addStatement("final int length1 = context.runLengths[context.runCount - 2]")
                    .addStatement("merge(context, positions, " + valuesArgs() + ", start1, length1, length2)")
                    .addStatement("context.runStarts[context.runCount - 2] = start1")
                    .addStatement("context.runLengths[context.runCount - 2] = length1 + length2")
                    .addStatement("context.runCount--")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitDoComparisonForColumn(final int k) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("doComparison" + k)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class);
            if (hasComparator[k]) {
                builder.addParameter(Comparator.class, "comparator");
            }
            builder.addParameter(column(k).elementType, "lhs")
                    .addParameter(column(k).elementType, "rhs");
            if (hasComparator[k]) {
                // the comparator is supplied in the ascending sense; descending columns reverse it at creation
                builder.addStatement("return comparator.compare(lhs, rhs)");
            } else {
                builder.addStatement("return $L", column(k).comparisonExpression(order[k]));
            }
            return builder.build();
        }

        /** The call to this column's doComparison, in terms of {@code lhsPos} and {@code rhsPos}. */
        private String doComparisonCall(final int k) {
            final String comparatorArg = hasComparator[k] ? "context.comparator" + k + ", " : "";
            return "doComparison" + k + "(" + comparatorArg
                    + "valuesToSort" + k + ".get(lhsPos), valuesToSort" + k + ".get(rhsPos))";
        }

        private MethodSpec emitCompareColumns() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("compareColumns")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class)
                    .addJavadoc("Compares the elements at two positions, column by column; later columns are only "
                            + "read when all\nearlier columns compare equal.\n");
            maybeAddContextParam(builder);
            addValuesParams(builder);
            builder.addParameter(int.class, "lhsPos")
                    .addParameter(int.class, "rhsPos");
            builder.addStatement("final int cmp0 = " + doComparisonCall(0));
            if (n == 1) {
                builder.addStatement("return cmp0");
                return builder.build();
            }
            for (int k = 1; k < n; ++k) {
                builder.beginControlFlow("if (cmp$L != 0)", k - 1);
                builder.addStatement("return cmp$L", k - 1);
                builder.endControlFlow();
                if (k < n - 1) {
                    builder.addStatement("final int cmp$L = " + doComparisonCall(k), k);
                } else {
                    builder.addStatement("return " + doComparisonCall(k));
                }
            }
            return builder.build();
        }

        private MethodSpec emitCompareOp(final String name, final String op) {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder(name)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(boolean.class);
            maybeAddContextParam(builder);
            addValuesParams(builder);
            return builder
                    .addParameter(int.class, "lhsPos")
                    .addParameter(int.class, "rhsPos")
                    .addStatement("return " + compareCall("lhsPos", "rhsPos") + " $L 0", op)
                    .build();
        }

        private MethodSpec emitEnsureMergeInvariants() {
            return kernelMethod("ensureMergeInvariants")
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
                    .addStatement("merge(context, positions, " + valuesArgs() + ", yStart, yLen, xLen)")
                    .addComment("unchanged: context.runStarts[yStart];")
                    .addStatement("context.runLengths[yIndex] += xLen")
                    .nextControlFlow("else")
                    .addComment("merge y and z")
                    .addStatement("final int zStart = context.runStarts[zIndex]")
                    .addStatement("merge(context, positions, " + valuesArgs() + ", zStart, zLen, yLen)")
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
            return kernelMethod("merge")
                    .addParameter(int.class, "start1")
                    .addParameter(int.class, "length1")
                    .addParameter(int.class, "length2")
                    .addComment(
                            "we know that we can never have zero length runs, because there is a minimum run size enforced; and at the")
                    .addComment(
                            "end of an input, we won't create a zero-length run. When we merge runs, they only become bigger, thus")
                    .addComment("they'll never be empty. I'm being cheap about function calls and control flow here.")
                    .addStatement("final int start2 = start1 + length1")
                    .addComment("find the location of run2[0] in run1")
                    .addStatement("final int run2loPos = positions.get(start2)")
                    .addStatement("final int mergeStartPosition = upperBound(" + contextArg()
                            + "positions, " + valuesArgs() + ", start1, start1 + length1, run2loPos)")
                    .beginControlFlow("if (mergeStartPosition == start1 + length1)")
                    .addComment("these two runs are sorted already")
                    .addStatement("return")
                    .endControlFlow()
                    .addComment("find the location of run1[length1 - 1] in run2")
                    .addStatement("final int run1hiPos = positions.get(start1 + length1 - 1)")
                    .addStatement("final int mergeEndPosition = lowerBound(" + contextArg()
                            + "positions, " + valuesArgs() + ", start2, start2 + length2, run1hiPos)")
                    .addComment("figure out which of the two runs is now shorter")
                    .addStatement("final int remaining1 = start1 + length1 - mergeStartPosition")
                    .addStatement("final int remaining2 = mergeEndPosition - start2")
                    .beginControlFlow("if (remaining1 < remaining2)")
                    .addStatement("copyToTemporary(context, positions, mergeStartPosition, remaining1)")
                    .addComment(
                            "now we need to do the merge from temporary and remaining2 into remaining1 (so start at the front,")
                    .addComment("because we've preserved all the values of run1")
                    .addStatement("frontMerge(context, positions, " + valuesArgs()
                            + ", mergeStartPosition, start2, remaining2)")
                    .nextControlFlow("else")
                    .addStatement("copyToTemporary(context, positions, start2, remaining2)")
                    .addComment(
                            "now we need to do the merge from temporary and remaining1 into the remaining two area (so start at the")
                    .addComment("back, because we've preserved all the values of run2)")
                    .addStatement("backMerge(context, positions, " + valuesArgs()
                            + ", mergeStartPosition, remaining1)")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitFrontMerge() {
            return kernelMethod("frontMerge")
                    .addJavadoc(MERGE_JAVADOC_FRONT)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "start2", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length2", Modifier.FINAL).build())
                    .addStatement("int tempCursor = 0")
                    .addStatement("int run2Cursor = start2")
                    .addStatement("final int run1size = context.temporaryPositions.size()")
                    .addStatement("int ii")
                    .addStatement("final int mergeEndExclusive = start2 + length2")
                    .addStatement("int val1Pos = context.temporaryPositions.get(tempCursor)")
                    .addStatement("int val2Pos = positions.get(run2Cursor)")
                    .addStatement("ii = mergeStartPosition")
                    .beginControlFlow("nodataleft: while (ii < mergeEndExclusive)")
                    .addStatement("int run1wins = 0")
                    .addStatement("int run2wins = 0")
                    .beginControlFlow("if (context.minGallop < 2)")
                    .addStatement("context.minGallop = 2")
                    .endControlFlow()
                    .beginControlFlow("while (run1wins < context.minGallop && run2wins < context.minGallop)")
                    .beginControlFlow("if (" + opCall("leq", "val1Pos", "val2Pos") + ")")
                    .addStatement("positions.set(ii++, val1Pos)")
                    .beginControlFlow("if (++tempCursor == run1size)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1Pos = context.temporaryPositions.get(tempCursor)")
                    .addStatement("run1wins++")
                    .addStatement("run2wins = 0")
                    .nextControlFlow("else")
                    .addStatement("positions.set(ii++, val2Pos)")
                    .beginControlFlow("if (++run2Cursor == mergeEndExclusive)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2Pos = positions.get(run2Cursor)")
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
                    .addStatement("final int copyUntil1 = upperBound(" + contextArg()
                            + "context.temporaryPositions, " + valuesArgs() + ", tempCursor, run1size, val2Pos)")
                    .addStatement("final int gallopLength1 = copyUntil1 - tempCursor")
                    .beginControlFlow("if (gallopLength1 > 0)")
                    .addStatement("copyToChunk(context.temporaryPositions, positions, tempCursor, ii, gallopLength1)")
                    .addStatement("tempCursor += gallopLength1")
                    .addStatement("ii += gallopLength1")
                    .beginControlFlow("if (tempCursor == run1size)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1Pos = context.temporaryPositions.get(tempCursor)")
                    .addStatement("context.minGallop--")
                    .endControlFlow()
                    .addComment(
                            "if we had a lot of things from run2, we take the next thing from run1 and then find it in run2")
                    .addStatement("final int copyUntil2 = lowerBound(" + contextArg() + "positions, " + valuesArgs()
                            + ", run2Cursor, mergeEndExclusive, val1Pos)")
                    .addStatement("final int gallopLength2 = copyUntil2 - run2Cursor")
                    .beginControlFlow("if (gallopLength2 > 0)")
                    .addStatement("copyToChunk(positions, positions, run2Cursor, ii, gallopLength2)")
                    .addStatement("run2Cursor += gallopLength2")
                    .addStatement("ii += gallopLength2")
                    .beginControlFlow("if (run2Cursor == mergeEndExclusive)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2Pos = positions.get(run2Cursor)")
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
                    .addStatement("positions.set(ii, context.temporaryPositions.get(tempCursor))")
                    .addStatement("tempCursor++")
                    .addStatement("ii++")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitBackMerge() {
            return kernelMethod("backMerge")
                    .addJavadoc(MERGE_JAVADOC_BACK)
                    .addParameter(ParameterSpec.builder(int.class, "mergeStartPosition", Modifier.FINAL).build())
                    .addParameter(ParameterSpec.builder(int.class, "length1", Modifier.FINAL).build())
                    .addStatement("final int run1End = mergeStartPosition + length1")
                    .addStatement("int run1Cursor = run1End - 1")
                    .addStatement("int tempCursor = context.temporaryPositions.size() - 1")
                    .addStatement("final int mergeLength = context.temporaryPositions.size() + length1")
                    .addStatement("int ii")
                    .addStatement("int val1Pos = positions.get(run1Cursor)")
                    .addStatement("int val2Pos = context.temporaryPositions.get(tempCursor)")
                    .addStatement("final int mergeEnd = mergeStartPosition + mergeLength")
                    .addStatement("ii = mergeEnd - 1")
                    .beginControlFlow("nodataleft: while (ii >= mergeStartPosition)")
                    .addStatement("int run1wins = 0")
                    .addStatement("int run2wins = 0")
                    .beginControlFlow("if (context.minGallop < 2)")
                    .addStatement("context.minGallop = 2")
                    .endControlFlow()
                    .beginControlFlow("while (run1wins < context.minGallop && run2wins < context.minGallop)")
                    .beginControlFlow("if (" + opCall("geq", "val2Pos", "val1Pos") + ")")
                    .addStatement("positions.set(ii--, val2Pos)")
                    .beginControlFlow("if (--tempCursor < 0)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2Pos = context.temporaryPositions.get(tempCursor)")
                    .addStatement("run2wins++")
                    .addStatement("run1wins = 0")
                    .nextControlFlow("else")
                    .addStatement("positions.set(ii--, val1Pos)")
                    .beginControlFlow("if (--run1Cursor < mergeStartPosition)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1Pos = positions.get(run1Cursor)")
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
                    .addStatement("final int copyUntil2 = lowerBound(" + contextArg()
                            + "context.temporaryPositions, " + valuesArgs() + ", 0, tempCursor, val1Pos) + 1")
                    .addStatement("final int gallopLength2 = tempCursor - copyUntil2 + 1")
                    .beginControlFlow("if (gallopLength2 > 0)")
                    .addStatement(
                            "copyToChunk(context.temporaryPositions, positions, copyUntil2, ii - gallopLength2 + 1, gallopLength2)")
                    .addStatement("tempCursor -= gallopLength2")
                    .addStatement("ii -= gallopLength2")
                    .beginControlFlow("if (tempCursor < 0)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val2Pos = context.temporaryPositions.get(tempCursor)")
                    .addStatement("context.minGallop--")
                    .endControlFlow()
                    .addComment(
                            "if we had a lot of things from run1, we take the next thing from run2 and then find it in run1")
                    .addStatement("final int copyUntil1 = upperBound(" + contextArg() + "positions, " + valuesArgs()
                            + ", mergeStartPosition, run1Cursor, val2Pos)")
                    .addStatement("final int gallopLength1 = run1Cursor - copyUntil1")
                    .beginControlFlow("if (gallopLength1 > 0)")
                    .addStatement(
                            "copyToChunk(positions, positions, copyUntil1, ii - gallopLength1, gallopLength1 + 1)")
                    .addStatement("run1Cursor -= gallopLength1")
                    .addStatement("ii -= gallopLength1")
                    .beginControlFlow("if (run1Cursor < mergeStartPosition)")
                    .addStatement("break nodataleft")
                    .endControlFlow()
                    .addStatement("val1Pos = positions.get(run1Cursor)")
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
                    .addStatement("positions.set(ii, context.temporaryPositions.get(tempCursor))")
                    .addStatement("tempCursor--")
                    .addStatement("ii--")
                    .endControlFlow()
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
                    .returns(int.class);
            maybeAddContextParam(builder);
            builder.addParameter(readablePositions, "positions");
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
                    .addStatement("return bound(" + contextArg() + "positions, " + valuesArgs()
                            + ", lo, hi, searchPos, false)")
                    .build();
        }

        private MethodSpec emitLowerBound() {
            return boundMethod("lowerBound")
                    .addComment(
                            "when we binary search in 2, we must identify a position for search value that is *before* our test values;")
                    .addComment("because the values from run 1 may never be inserted after an equal value from run 2")
                    .addStatement("return bound(" + contextArg() + "positions, " + valuesArgs()
                            + ", lo, hi, searchPos, true)")
                    .build();
        }

        private MethodSpec emitBound() {
            return boundMethod("bound")
                    .addParameter(ParameterSpec.builder(boolean.class, "lower", Modifier.FINAL).build())
                    .addComment("lt or leq")
                    .addStatement("final int compareLimit = lower ? -1 : 0")
                    .beginControlFlow("while (lo < hi)")
                    .addStatement("final int mid = (lo + hi) >>> 1")
                    .addStatement("final boolean moveLo = " + compareCall("positions.get(mid)", "searchPos")
                            + " <= compareLimit")
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
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("insertionSort")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC);
            maybeAddContextParam(builder);
            builder.addParameter(writablePositions, "positions");
            addValuesParams(builder);
            return builder
                    .addParameter(int.class, "offset")
                    .addParameter(int.class, "length")
                    .beginControlFlow("for (int ii = offset + 1; ii < offset + length; ++ii)")
                    .beginControlFlow("for (int jj = ii; jj > offset && "
                            + opCall("gt", "positions.get(jj - 1)", "positions.get(jj)") + "; jj--)")
                    .addStatement("swap(positions, jj, jj - 1)")
                    .endControlFlow()
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitSwap() {
            return MethodSpec.methodBuilder("swap")
                    .addModifiers(Modifier.STATIC, Modifier.PRIVATE)
                    .addParameter(writablePositions, "positions")
                    .addParameter(int.class, "a")
                    .addParameter(int.class, "b")
                    .addStatement("final int tempPos = positions.get(a)")
                    .addStatement("positions.set(a, positions.get(b))")
                    .addStatement("positions.set(b, tempPos)")
                    .build();
        }
    }
}
