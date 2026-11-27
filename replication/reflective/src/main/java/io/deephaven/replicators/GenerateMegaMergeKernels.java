//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.replicators;

import com.palantir.javapoet.AnnotationSpec;
import com.palantir.javapoet.ClassName;
import com.palantir.javapoet.FieldSpec;
import com.palantir.javapoet.JavaFile;
import com.palantir.javapoet.MethodSpec;
import com.palantir.javapoet.ParameterSpec;
import com.palantir.javapoet.ParameterizedTypeName;
import com.palantir.javapoet.TypeName;
import com.palantir.javapoet.TypeSpec;
import com.palantir.javapoet.TypeVariableName;
import com.palantir.javapoet.WildcardTypeName;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Indices;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sort.LongMegaMergeKernel;
import io.deephaven.engine.table.impl.sort.timsort.TimsortUtils;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.CharacterArraySource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.replicators.GenerateTimsortKernels.ChunkFamily;
import io.deephaven.replicators.GenerateTimsortKernels.Direction;
import io.deephaven.replicators.GenerateTimsortKernels.KeyKind;

import javax.lang.model.element.Modifier;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static io.deephaven.replication.ReplicationUtils.replicatedDir;

/**
 * Generates the MegaMerge kernels, which merge a sorted chunk into a sorted array source with a parallel array of row
 * keys, for each value type and sort direction using JavaPoet.
 *
 * <p>
 * The value types and their comparisons are those of {@link GenerateTimsortKernels}, so a MegaMerge kernel orders
 * values exactly as the timsort kernel that sorted the chunks being merged.
 */
public class GenerateMegaMergeKernels {
    private static final String TASK = "generateMegaMergeKernels";

    private static final String PACKAGE = "io.deephaven.engine.table.impl.sort.megamerge";
    private static final File SOURCE_ROOT = new File(replicatedDir("engine/table", TASK));

    private static final ClassName ANY = ClassName.get(Any.class);
    private static final ClassName INDICES = ClassName.get(Indices.class);
    private static final ClassName CHUNK = ClassName.get(Chunk.class);
    private static final ClassName LONG_CHUNK = ClassName.get(LongChunk.class);
    private static final ClassName LONG_ARRAY_SOURCE = ClassName.get(LongArraySource.class);
    private static final ClassName WRITABLE_COLUMN_SOURCE = ClassName.get(WritableColumnSource.class);
    private static final ClassName LONG_MEGA_MERGE_KERNEL = ClassName.get(LongMegaMergeKernel.class);
    private static final ClassName TIMSORT_UTILS = ClassName.get(TimsortUtils.class);

    private static final AnnotationSpec SAME_PARAMETER_VALUE = AnnotationSpec.builder(SuppressWarnings.class)
            .addMember("value", "$S", "SameParameterValue")
            .build();

    /** A value type to generate kernels for, with the array source that holds the merged values. */
    static final class ValueKind {
        final KeyKind keyKind;
        final TypeName arraySource;

        ValueKind(final KeyKind keyKind, final TypeName arraySource) {
            this.keyKind = keyKind;
            this.arraySource = arraySource;
        }
    }

    private static final List<ValueKind> VALUE_KINDS = List.of(
            new ValueKind(GenerateTimsortKernels.CHAR_KIND, ClassName.get(CharacterArraySource.class)),
            new ValueKind(GenerateTimsortKernels.BYTE_KIND, ClassName.get(ByteArraySource.class)),
            new ValueKind(GenerateTimsortKernels.SHORT_KIND, ClassName.get(ShortArraySource.class)),
            new ValueKind(GenerateTimsortKernels.INT_KIND, ClassName.get(IntegerArraySource.class)),
            new ValueKind(GenerateTimsortKernels.LONG_KIND, ClassName.get(LongArraySource.class)),
            new ValueKind(GenerateTimsortKernels.FLOAT_KIND, ClassName.get(FloatArraySource.class)),
            new ValueKind(GenerateTimsortKernels.DOUBLE_KIND, ClassName.get(DoubleArraySource.class)),
            new ValueKind(GenerateTimsortKernels.OBJECT_KIND,
                    ParameterizedTypeName.get(ClassName.get(ObjectArraySource.class), ClassName.OBJECT)));

    public static void main(String[] args) throws IOException {
        for (final ValueKind valueKind : VALUE_KINDS) {
            for (final Direction direction : Direction.values()) {
                final KernelEmitter emitter = new KernelEmitter(valueKind, direction);
                writeFile(emitter.emit(), emitter.kernelClass.simpleName());
            }
        }
    }

    private static void writeFile(final TypeSpec type, final String className) throws IOException {
        final JavaFile.Builder fileBuilder = JavaFile.builder(PACKAGE, type).indent("    ");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY\n");
        fileBuilder.addFileComment("****** Run GenerateMegaMergeKernels or ./gradlew " + TASK + " to regenerate\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("@formatter:off");
        System.out.println("Generating " + className + " to " + SOURCE_ROOT);
        fileBuilder.build().writeTo(SOURCE_ROOT);
    }

    /** Emits one kernel class: a stateless context singleton and static merge, gallop, and search methods. */
    static final class KernelEmitter {
        private final ValueKind valueKind;
        private final ChunkFamily chunks;
        private final Direction direction;

        private final TypeVariableName attr = TypeVariableName.get("ATTR", ANY);
        private final TypeVariableName keyIndices = TypeVariableName.get("KEY_INDICES", INDICES);
        private final ClassName kernelClass;
        private final ClassName contextClass;

        KernelEmitter(final ValueKind valueKind, final Direction direction) {
            this.valueKind = valueKind;
            this.chunks = valueKind.keyKind.chunks;
            this.direction = direction;

            final String className = valueKind.keyKind.namePart
                    + (direction == Direction.DESCENDING ? "LongMegaMergeDescendingKernel" : "LongMegaMergeKernel");
            this.kernelClass = ClassName.get(PACKAGE, className);
            this.contextClass = kernelClass.nestedClass(className + "Context");
        }

        private TypeName valueType() {
            return chunks.elementType;
        }

        private TypeName valueSource() {
            return valueKind.arraySource;
        }

        private TypeName keysChunk() {
            return ParameterizedTypeName.get(LONG_CHUNK, keyIndices);
        }

        TypeSpec emit() {
            return TypeSpec.classBuilder(kernelClass)
                    .addModifiers(Modifier.PUBLIC)
                    .addJavadoc("Merges a sorted chunk of $L into a sorted run of $L in $L order, moving the "
                            + "parallel long row keys\nwith their values.\n",
                            chunks.plural, chunks.plural,
                            direction == Direction.DESCENDING ? "descending" : "ascending")
                    .addMethod(MethodSpec.constructorBuilder()
                            .addModifiers(Modifier.PRIVATE)
                            .addStatement("throw new $T()", UnsupportedOperationException.class)
                            .build())
                    .addType(emitContext())
                    .addMethod(emitCreateContext())
                    .addMethod(emitMerge())
                    .addMethod(emitCopyChunkToDest())
                    .addMethod(emitMoveInDest())
                    .addMethod(emitDoComparison())
                    .addMethod(emitGeq())
                    .addMethod(emitUpperBound())
                    .addMethod(emitSourceBound())
                    .addMethod(emitSourceGallopBackward())
                    .addMethod(emitChunkGallopBackward())
                    .addMethod(emitChunkBound())
                    .build();
        }

        private TypeSpec emitContext() {
            final MethodSpec.Builder merge = MethodSpec.methodBuilder("merge")
                    .addAnnotation(Override.class)
                    .addModifiers(Modifier.PUBLIC)
                    .addParameter(LONG_ARRAY_SOURCE, "indexDestinationSource")
                    .addParameter(ParameterizedTypeName.get(WRITABLE_COLUMN_SOURCE, WildcardTypeName.subtypeOf(
                            ClassName.OBJECT)), "valuesDestinationSource")
                    .addParameter(long.class, "destinationOffset")
                    .addParameter(long.class, "destinationSize")
                    .addParameter(keysChunk(), "indexKeys")
                    .addParameter(ParameterizedTypeName.get(CHUNK, attr), "valuesToMerge")
                    .addStatement("$T.merge(indexDestinationSource, ($T) valuesDestinationSource, destinationOffset, "
                            + "destinationSize, indexKeys, valuesToMerge.as$LChunk())",
                            kernelClass, valueSource(), chunks.name);
            if (chunks.isObject) {
                merge.addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                        .addMember("value", "$S", "unchecked")
                        .build());
            }

            return TypeSpec.classBuilder(contextClass)
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(attr)
                    .addTypeVariable(keyIndices)
                    .addSuperinterface(ParameterizedTypeName.get(LONG_MEGA_MERGE_KERNEL, attr, keyIndices))
                    .addField(FieldSpec.builder(contextClass, "INSTANCE", Modifier.PRIVATE, Modifier.STATIC,
                            Modifier.FINAL)
                            .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                                    .addMember("value", "$S", "rawtypes")
                                    .build())
                            .initializer("new $T()", contextClass)
                            .build())
                    .addMethod(merge.build())
                    .build();
        }

        private MethodSpec emitCreateContext() {
            return MethodSpec.methodBuilder("createContext")
                    .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                            .addMember("value", "$S", "unchecked")
                            .build())
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(attr)
                    .addTypeVariable(keyIndices)
                    .returns(ParameterizedTypeName.get(contextClass, attr, keyIndices))
                    .addStatement("return $T.INSTANCE", contextClass)
                    .build();
        }

        private MethodSpec emitMerge() {
            final TypeName valueType = valueType();
            return MethodSpec.methodBuilder("merge")
                    .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                    .addTypeVariable(attr)
                    .addTypeVariable(keyIndices)
                    .addParameter(LONG_ARRAY_SOURCE, "destinationKeys")
                    .addParameter(valueSource(), "destinationValues")
                    .addParameter(long.class, "destinationOffset")
                    .addParameter(long.class, "destinationSize")
                    .addParameter(keysChunk(), "keysChunk")
                    .addParameter(chunks.chunkOf(attr), "valuesChunk")
                    .addStatement("destinationKeys.ensureCapacity(destinationOffset + destinationSize + "
                            + "keysChunk.size(), false)")
                    .addStatement("destinationValues.ensureCapacity(destinationOffset + destinationSize + "
                            + "valuesChunk.size(), false)")
                    .addCode("\n")
                    .addComment("find the location of run2[0] in run1")
                    .addStatement("final $T run2lo = valuesChunk.get(0)", valueType)
                    .addStatement("final long mergeStartPosition =\n"
                            + "upperBound(destinationValues, destinationOffset, destinationOffset + destinationSize, "
                            + "run2lo)")
                    .addCode("\n")
                    .beginControlFlow("if (mergeStartPosition == destinationOffset + destinationSize)")
                    .addStatement("copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, 0,\n"
                            + "destinationSize + destinationOffset, valuesChunk.size())")
                    .addStatement("return")
                    .endControlFlow()
                    .addCode("\n")
                    .addStatement("final long mergeLength = destinationSize + valuesChunk.size()")
                    .addCode("\n")
                    .addStatement("long destCursor = destinationOffset + destinationSize - 1")
                    .addStatement("int chunkCursor = keysChunk.size() - 1")
                    .addCode("\n")
                    .addStatement("$T val1 = destinationValues.getUnsafe(destCursor)", valueType)
                    .addStatement("$T val2 = valuesChunk.get(chunkCursor)", valueType)
                    .addCode("\n")
                    .addStatement("final long mergeEnd = destinationOffset + mergeLength")
                    .addCode("\n")
                    .addStatement("long ii = mergeEnd - 1")
                    .addCode("\n")
                    .addStatement("int minGallop = $T.INITIAL_GALLOP", TIMSORT_UTILS)
                    .addCode("\n")
                    .beginControlFlow("no_data_left: while (ii >= mergeStartPosition)")
                    .addStatement("int destWins = 0")
                    .addStatement("int chunkWins = 0")
                    .addCode("\n")
                    .beginControlFlow("if (minGallop < 2)")
                    .addStatement("minGallop = 2")
                    .endControlFlow()
                    .addCode("\n")
                    .beginControlFlow("while (destWins < minGallop && chunkWins < minGallop)")
                    .beginControlFlow("if (geq(val2, val1))")
                    .addStatement("destinationValues.set(ii, val2)")
                    .addStatement("destinationKeys.set(ii--, keysChunk.get(chunkCursor))")
                    .addCode("\n")
                    .beginControlFlow("if (--chunkCursor < 0)")
                    .addStatement("break no_data_left")
                    .endControlFlow()
                    .addStatement("val2 = valuesChunk.get(chunkCursor)")
                    .addCode("\n")
                    .addStatement("chunkWins++")
                    .addStatement("destWins = 0")
                    .nextControlFlow("else")
                    .addStatement("destinationValues.set(ii, val1)")
                    .addStatement("destinationKeys.set(ii--, destinationKeys.getLong(destCursor))")
                    .addCode("\n")
                    .beginControlFlow("if (--destCursor < (int) mergeStartPosition)")
                    .addStatement("break no_data_left")
                    .endControlFlow()
                    .addStatement("val1 = destinationValues.getUnsafe(destCursor)")
                    .addCode("\n")
                    .addStatement("destWins++")
                    .addStatement("chunkWins = 0")
                    .endControlFlow()
                    .endControlFlow()
                    .addCode("\n")
                    .addComment("we are in galloping mode now, if we had run out of data then we should have already "
                            + "bailed out to")
                    .addComment("no_data_left")
                    .beginControlFlow("while (ii >= mergeStartPosition)")
                    .addComment("if we had a lot of things from run2, we take the next thing from run1 then find it "
                            + "in run2")
                    .addStatement("final int copyUntil2 = gallopBackward(valuesChunk, 0, chunkCursor, val1, true)")
                    .addCode("\n")
                    .addStatement("final int gallopLength2 = chunkCursor - copyUntil2 + 1")
                    .beginControlFlow("if (gallopLength2 > 1)")
                    .addStatement("copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, "
                            + "copyUntil2,\nii - gallopLength2 + 1, gallopLength2)")
                    .addStatement("chunkCursor -= gallopLength2")
                    .addStatement("ii -= gallopLength2")
                    .addCode("\n")
                    .beginControlFlow("if (chunkCursor < 0)")
                    .addStatement("break no_data_left")
                    .endControlFlow()
                    .addCode("\n")
                    .addStatement("val2 = valuesChunk.get(chunkCursor)")
                    .addCode("\n")
                    .addStatement("minGallop--")
                    .endControlFlow()
                    .addCode("\n")
                    .addComment("if we had a lot of things from run1, we take the next thing from run2 and then find "
                            + "it in run1")
                    .addStatement("final long copyUntil1 = gallopBackward(destinationValues, mergeStartPosition, "
                            + "destCursor, val2, false)")
                    .addCode("\n")
                    .addStatement("final long gallopLength1 = destCursor - copyUntil1 + 1")
                    .beginControlFlow("if (gallopLength1 > 1)")
                    .addStatement("moveInDest(destinationKeys, destinationValues, copyUntil1, ii - gallopLength1 + 1, "
                            + "gallopLength1)")
                    .addStatement("destCursor -= gallopLength1")
                    .addStatement("ii -= gallopLength1")
                    .addCode("\n")
                    .beginControlFlow("if (destCursor < mergeStartPosition)")
                    .addStatement("break no_data_left")
                    .endControlFlow()
                    .addStatement("val1 = destinationValues.getUnsafe(destCursor)")
                    .addStatement("minGallop--")
                    .endControlFlow()
                    .addCode("\n")
                    .beginControlFlow(
                            "if (gallopLength1 < $T.INITIAL_GALLOP\n$>$>&& gallopLength2 < $T.INITIAL_GALLOP$<$<)",
                            TIMSORT_UTILS, TIMSORT_UTILS)
                    .addComment("undo the possible subtraction from above")
                    .addStatement("minGallop += 2")
                    .addStatement("break")
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow()
                    .addCode("\n")
                    .beginControlFlow("if (chunkCursor >= 0)")
                    .addStatement("copyChunkToDest(keysChunk, valuesChunk, destinationKeys, destinationValues, 0, "
                            + "ii - chunkCursor,\nchunkCursor + 1)")
                    .endControlFlow()
                    .build();
        }

        private MethodSpec emitCopyChunkToDest() {
            return MethodSpec.methodBuilder("copyChunkToDest")
                    .addAnnotation(AnnotationSpec.builder(SuppressWarnings.class)
                            .addMember("value", "$S", "rawtypes")
                            .build())
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addTypeVariable(attr)
                    .addTypeVariable(keyIndices)
                    .addParameter(keysChunk(), "keysChunk")
                    .addParameter(chunks.chunkOf(attr), "valuesChunk")
                    .addParameter(LONG_ARRAY_SOURCE, "destinationKeys")
                    .addParameter(valueSource(), "destinationValues")
                    .addParameter(int.class, "sourceStart")
                    .addParameter(long.class, "destStart")
                    .addParameter(int.class, "length")
                    // the values chunk's attribute is not Values, which copyFromChunk requires
                    .addStatement("destinationValues.copyFromChunk(destStart, length, ($T) valuesChunk, sourceStart)",
                            chunks.chunkName)
                    .addStatement("destinationKeys.copyFromChunk(destStart, length, keysChunk, sourceStart)")
                    .build();
        }

        private MethodSpec emitMoveInDest() {
            return MethodSpec.methodBuilder("moveInDest")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .addParameter(LONG_ARRAY_SOURCE, "destinationKeys")
                    .addParameter(valueSource(), "destinationValues")
                    .addParameter(long.class, "sourceStart")
                    .addParameter(long.class, "destStart")
                    .addParameter(long.class, "length")
                    .addStatement("destinationKeys.move(sourceStart, destStart, length)")
                    .addStatement("destinationValues.move(sourceStart, destStart, length)")
                    .build();
        }

        private MethodSpec emitDoComparison() {
            final MethodSpec.Builder builder = MethodSpec.methodBuilder("doComparison")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(int.class)
                    .addParameter(valueType(), "lhs")
                    .addParameter(valueType(), "rhs");
            if (direction == Direction.DESCENDING) {
                builder.addComment("note that this is a descending kernel, thus the comparisons here are backwards");
            }
            return builder
                    .addStatement("return $L", valueKind.keyKind.comparison.comparisonExpression(direction))
                    .build();
        }

        private MethodSpec emitGeq() {
            return MethodSpec.methodBuilder("geq")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(boolean.class)
                    .addParameter(valueType(), "lhs")
                    .addParameter(valueType(), "rhs")
                    .addStatement("return doComparison(lhs, rhs) >= 0")
                    .build();
        }

        private ParameterSpec lowerParam() {
            return ParameterSpec.builder(boolean.class, "lower", Modifier.FINAL)
                    .addAnnotation(SAME_PARAMETER_VALUE)
                    .build();
        }

        private MethodSpec emitUpperBound() {
            return MethodSpec.methodBuilder("upperBound")
                    .addJavadoc("When we binary search in run 1, we must identify a position for the search value "
                            + "that is <em>after</em> our\ntest values, because the values from run 2 may never be "
                            + "inserted before an equal value from run 1.\n"
                            + "\n@param lo the inclusive lower bound of the search"
                            + "\n@param hi the exclusive upper bound of the search"
                            + "\n@return the position of the first element that is > searchValue, or hi if there is "
                            + "no such element\n")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(long.class)
                    .addParameter(valueSource(), "values")
                    .addParameter(long.class, "lo")
                    .addParameter(long.class, "hi")
                    .addParameter(valueType(), "searchValue")
                    .addStatement("return bound(values, lo, hi, searchValue, false)")
                    .build();
        }

        private MethodSpec emitSourceBound() {
            return emitBound(valueSource(), long.class, "getUnsafe");
        }

        private MethodSpec emitChunkBound() {
            return emitBound(chunks.chunkOfWildcard(), int.class, "get");
        }

        private MethodSpec emitBound(final TypeName valuesType, final Class<?> positionType, final String getter) {
            return MethodSpec.methodBuilder("bound")
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(positionType)
                    .addParameter(valuesType, "valuesToSort")
                    .addParameter(positionType, "lo")
                    .addParameter(positionType, "hi")
                    .addParameter(valueType(), "searchValue")
                    .addParameter(lowerParam())
                    .addComment("lt or leq")
                    .addStatement("final int compareLimit = lower ? -1 : 0")
                    .addCode("\n")
                    .beginControlFlow("while (lo < hi)")
                    .addStatement("final $T mid = (lo + hi) >>> 1", positionType)
                    .addStatement("final $T testValue = valuesToSort.$L(mid)", valueType(), getter)
                    .addStatement("final boolean moveLo = doComparison(testValue, searchValue) <= compareLimit")
                    .beginControlFlow("if (moveLo)")
                    .addComment("For bound, (testValue OP searchValue) means that the result somewhere later than "
                            + "'mid' [OP=lt or leq]")
                    .addStatement("lo = mid + 1")
                    .nextControlFlow("else")
                    .addStatement("hi = mid")
                    .endControlFlow()
                    .endControlFlow()
                    .addCode("\n")
                    .addStatement("return lo")
                    .build();
        }

        private static final String GALLOP_BACKWARD_JAVADOC =
                "Returns {@code bound(valuesToSort, lo, hi, searchValue, lower)}, probing hi - 1, hi - 2, hi - 4, "
                        + "hi - 8, ... before\nthe binary search, so the number of comparisons is logarithmic in the "
                        + "distance from the result to hi.\n<p>\n";

        private MethodSpec emitSourceGallopBackward() {
            return emitGallopBackward(valueSource(), long.class, "getUnsafe", false,
                    "The search in run 1 passes lower = false, as values from run 2 may never be inserted before an "
                            + "equal value from\nrun 1.\n");
        }

        private MethodSpec emitChunkGallopBackward() {
            return emitGallopBackward(chunks.chunkOfWildcard(), int.class, "get", true,
                    "The search in run 2 passes lower = true, as values from run 1 may never be inserted after an "
                            + "equal value from\nrun 2.\n");
        }

        private MethodSpec emitGallopBackward(final TypeName valuesType, final Class<?> positionType,
                final String getter, final boolean loSameValue, final String javadocTail) {
            final ParameterSpec.Builder lo = ParameterSpec.builder(positionType, "lo");
            if (loSameValue) {
                lo.addAnnotation(SAME_PARAMETER_VALUE);
            }
            return MethodSpec.methodBuilder("gallopBackward")
                    .addJavadoc(GALLOP_BACKWARD_JAVADOC + javadocTail)
                    .addModifiers(Modifier.PRIVATE, Modifier.STATIC)
                    .returns(positionType)
                    .addParameter(valuesType, "valuesToSort")
                    .addParameter(lo.build())
                    .addParameter(positionType, "hi")
                    .addParameter(valueType(), "searchValue")
                    .addParameter(lowerParam())
                    .addComment("lt or leq")
                    .addStatement("final int compareLimit = lower ? -1 : 0")
                    .addStatement("final $T maxDistance = hi - lo", positionType)
                    .addCode("\n")
                    .addComment("every element at or after searchHi is after the result")
                    .addStatement("$T searchHi = hi", positionType)
                    .addStatement("$T distance = 0", positionType)
                    .beginControlFlow("while (distance < maxDistance\n$>$>&& doComparison(valuesToSort.$L(hi - 1 - "
                            + "distance), searchValue) > compareLimit$<$<)", getter)
                    .addStatement("searchHi = hi - 1 - distance")
                    .addStatement("distance = distance < (maxDistance >> 1) ? (distance << 1) + 1 : maxDistance")
                    .endControlFlow()
                    .addCode("\n")
                    .addStatement("return bound(valuesToSort, hi - distance, searchHi, searchValue, lower)")
                    .build();
        }
    }
}
