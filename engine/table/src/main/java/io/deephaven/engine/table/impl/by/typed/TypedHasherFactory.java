/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.*;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.StaticAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.TypedAsOfJoinFactory;
import io.deephaven.engine.table.impl.naturaljoin.RightIncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.naturaljoin.IncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.StaticNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.TypedNaturalJoinFactory;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.immutable.*;
import io.deephaven.engine.table.impl.updateby.hashing.UpdateByStateManagerTypedBase;
import io.deephaven.engine.table.impl.updateby.hashing.TypedUpdateByFactory;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.CharComparisons;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.lang.model.element.Modifier;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Produces typed hashers (e.g. byte * object * float * double * int) on-demand or using a set of pregenerated and
 * precompiled hashers for singleton and pairs of types.
 */
public class TypedHasherFactory {
    private static final boolean USE_PREGENERATED_HASHERS =
            Configuration.getInstance().getBooleanWithDefault("TypedHasherFactory.usePregeneratedHashers", true);

    /**
     * Produce a hasher for the given base class and column sources.
     *
     * @param <T> the base class
     * @param baseClass the base class (e.g. {@link StaticChunkedOperatorAggregationStateManagerTypedBase} that the
     *        generated hasher extends from
     * @param tableKeySources the key sources
     * @param originalKeySources
     * @param tableSize the initial table size
     * @param maximumLoadFactor the maximum load factor of the for the table
     * @param targetLoadFactor the load factor that we will rehash to
     * @return an instantiated hasher
     */
    public static <T> T make(Class<T> baseClass, ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] originalKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        return make(hasherConfigForBase(baseClass), tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                targetLoadFactor);
    }

    @NotNull
    public static <T> HasherConfig<T> hasherConfigForBase(Class<T> baseClass) {
        final HasherConfig.Builder<T> builder = new HasherConfig.Builder<>(baseClass);

        if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
            configureAggregation(builder);
            builder.classPrefix("StaticAggHasher").packageMiddle("staticagg");
            builder.overflowOrAlternateStateName("overflowOutputPosition");
            builder.openAddressed(false);
        } else if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
            configureAggregation(builder);
            builder.classPrefix("StaticAggOpenHasher").packageMiddle("staticopenagg");
            builder.openAddressed(true).openAddressedAlternate(false);
            builder.moveMainFull(TypedAggregationFactory::staticAggMoveMain);
            builder.addBuild(
                    new HasherConfig.BuildSpec("build", "outputPosition", false, true,
                            TypedAggregationFactory::buildFound,
                            TypedAggregationFactory::buildInsert));
        } else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
            configureAggregation(builder);
            builder.classPrefix("IncrementalAggHasher").packageMiddle("incagg");
            builder.openAddressed(false);
            builder.overflowOrAlternateStateName("overflowOutputPosition");
        } else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
            configureAggregation(builder);
            builder.classPrefix("IncrementalAggOpenHasher").packageMiddle("incopenagg");
            builder.overflowOrAlternateStateName("alternateOutputPosition");
            builder.moveMainFull(TypedAggregationFactory::incAggMoveMain);
            builder.moveMainAlternate(TypedAggregationFactory::incAggMoveMain);
            builder.alwaysMoveMain(true);

            final ClassName rowKeyType = ClassName.get(RowKeys.class);
            final ParameterizedTypeName emptiedChunkType =
                    ParameterizedTypeName.get(ClassName.get(WritableIntChunk.class), rowKeyType);

            builder.addProbe(new HasherConfig.ProbeSpec("probe", "outputPosition", false,
                    TypedAggregationFactory::probeFound, TypedAggregationFactory::probeMissing));

            builder.addBuild(new HasherConfig.BuildSpec("build", "outputPosition", false, true,
                    TypedAggregationFactory::buildFound, TypedAggregationFactory::buildInsertIncremental));
        } else if (baseClass.equals(StaticNaturalJoinStateManagerTypedBase.class)) {
            builder.classPrefix("StaticNaturalJoinHasher").packageGroup("naturaljoin").packageMiddle("staticopen")
                    .openAddressedAlternate(false)
                    .stateType(long.class).mainStateName("mainRightRowKey")
                    .emptyStateName("EMPTY_RIGHT_STATE")
                    .includeOriginalSources(true)
                    .supportRehash(false);

            final TypeName longArraySource = TypeName.get(LongArraySource.class);
            final ParameterSpec leftHashSlots = ParameterSpec.builder(longArraySource, "leftHashSlots").build();
            final ParameterSpec hashSlotOffset = ParameterSpec.builder(int.class, "hashSlotOffset").build();

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rightSideSentinel",
                    false, true, TypedNaturalJoinFactory::staticBuildLeftFound,
                    TypedNaturalJoinFactory::staticBuildLeftInsert,
                    leftHashSlots, hashSlotOffset));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateLeftSide", "rightRowKey",
                    false, TypedNaturalJoinFactory::staticProbeDecorateLeftFound,
                    TypedNaturalJoinFactory::staticProbeDecorateLeftMissing,
                    ParameterSpec.builder(longArraySource, "leftRedirections").build(), hashSlotOffset));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "rightSideSentinel",
                    true, true, TypedNaturalJoinFactory::staticBuildRightFound,
                    TypedNaturalJoinFactory::staticBuildRightInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateWithRightSide", "existingStateValue",
                    true, TypedNaturalJoinFactory::staticProbeDecorateRightFound, null));
        } else if (baseClass.equals(RightIncrementalNaturalJoinStateManagerTypedBase.class)) {
            builder.classPrefix("RightIncrementalNaturalJoinHasher").packageGroup("naturaljoin")
                    .packageMiddle("rightincopen")
                    .openAddressedAlternate(false)
                    .stateType(RowSet.class).mainStateName("leftRowSet")
                    .emptyStateName("null")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .moveMainFull(TypedNaturalJoinFactory::rightIncrementalMoveMain)
                    .addExtraPartialRehashParameter(
                            ParameterSpec.builder(NaturalJoinModifiedSlotTracker.class, "modifiedSlotTracker").build())
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedNaturalJoinFactory::rightIncrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "leftRowSetForState",
                    true, true, TypedNaturalJoinFactory::rightIncrementalBuildLeftFound,
                    TypedNaturalJoinFactory::rightIncrementalBuildLeftInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("addRightSide", null, true,
                    TypedNaturalJoinFactory::rightIncrementalRightFound,
                    null));


            final TypeName modifiedSlotTracker = TypeName.get(NaturalJoinModifiedSlotTracker.class);
            final ParameterSpec modifiedSlotTrackerParam =
                    ParameterSpec.builder(modifiedSlotTracker, "modifiedSlotTracker").build();

            builder.addProbe(new HasherConfig.ProbeSpec("removeRight", null, true,
                    TypedNaturalJoinFactory::rightIncrementalRemoveFound,
                    null,
                    modifiedSlotTrackerParam));

            builder.addProbe(new HasherConfig.ProbeSpec("addRightSide", null, true,
                    TypedNaturalJoinFactory::rightIncrementalAddFound,
                    null,
                    modifiedSlotTrackerParam));

            builder.addProbe(new HasherConfig.ProbeSpec("modifyByRight", null, true,
                    TypedNaturalJoinFactory::rightIncrementalModify,
                    null,
                    modifiedSlotTrackerParam));

            builder.addProbe(new HasherConfig.ProbeSpec("applyRightShift", null, true,
                    TypedNaturalJoinFactory::rightIncrementalShift,
                    null,
                    ParameterSpec.builder(long.class, "shiftDelta").build(),
                    modifiedSlotTrackerParam));
        } else if (baseClass.equals(IncrementalNaturalJoinStateManagerTypedBase.class)) {
            final ParameterSpec modifiedSlotTrackerParam =
                    ParameterSpec.builder(NaturalJoinModifiedSlotTracker.class, "modifiedSlotTracker").build();

            builder.classPrefix("IncrementalNaturalJoinHasher").packageGroup("naturaljoin")
                    .packageMiddle("incopen")
                    .openAddressedAlternate(true)
                    .stateType(long.class).mainStateName("mainRightRowKey")
                    .overflowOrAlternateStateName("alternateRightRowKey")
                    .emptyStateName("EMPTY_RIGHT_STATE")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .addExtraPartialRehashParameter(modifiedSlotTrackerParam)
                    .moveMainFull(TypedNaturalJoinFactory::incrementalMoveMainFull)
                    .moveMainAlternate(TypedNaturalJoinFactory::incrementalMoveMainAlternate)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedNaturalJoinFactory::incrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rightRowKeyForState",
                    true, false, TypedNaturalJoinFactory::incrementalBuildLeftFound,
                    TypedNaturalJoinFactory::incrementalBuildLeftInsert));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "existingRightRowKey", true,
                    false, TypedNaturalJoinFactory::incrementalRightFound,
                    TypedNaturalJoinFactory::incrementalRightInsert));


            builder.addProbe(new HasherConfig.ProbeSpec("removeRight", "existingRightRowKey", true,
                    TypedNaturalJoinFactory::incrementalRemoveRightFound,
                    TypedNaturalJoinFactory::incrementalRemoveRightMissing,
                    modifiedSlotTrackerParam));

            builder.addBuild(new HasherConfig.BuildSpec("addRightSide", "existingRightRowKey", true,
                    true, TypedNaturalJoinFactory::incrementalRightFoundUpdate,
                    TypedNaturalJoinFactory::incrementalRightInsertUpdate,
                    modifiedSlotTrackerParam));

            builder.addProbe(new HasherConfig.ProbeSpec("modifyByRight", "existingRightRowKey", false,
                    TypedNaturalJoinFactory::incrementalModifyRightFound,
                    TypedNaturalJoinFactory::incrementalModifyRightMissing,
                    modifiedSlotTrackerParam));

            ParameterSpec probeContextParam =
                    ParameterSpec.builder(IncrementalNaturalJoinStateManagerTypedBase.ProbeContext.class, "pc").build();
            builder.addProbe(new HasherConfig.ProbeSpec("applyRightShift", "existingRightRowKey", true,
                    TypedNaturalJoinFactory::incrementalApplyRightShift,
                    TypedNaturalJoinFactory::incrementalApplyRightShiftMissing,
                    ParameterSpec.builder(long.class, "shiftDelta").build(),
                    modifiedSlotTrackerParam, probeContextParam));

            builder.addBuild(new HasherConfig.BuildSpec("addLeftSide", "rightRowKeyForState", true,
                    true, TypedNaturalJoinFactory::incrementalLeftFoundUpdate,
                    TypedNaturalJoinFactory::incrementalLeftInsertUpdate,
                    ParameterSpec.builder(TypeName.get(LongArraySource.class), "leftRedirections").build(),
                    ParameterSpec.builder(long.class, "leftRedirectionOffset").build()));

            builder.addProbe(new HasherConfig.ProbeSpec("removeLeft", null, true,
                    TypedNaturalJoinFactory::incrementalRemoveLeftFound,
                    TypedNaturalJoinFactory::incrementalRemoveLeftMissing));

            builder.addProbe(new HasherConfig.ProbeSpec("applyLeftShift", null, true,
                    TypedNaturalJoinFactory::incrementalShiftLeftFound,
                    TypedNaturalJoinFactory::incrementalShiftLeftMissing,
                    ParameterSpec.builder(long.class, "shiftDelta").build(),
                    probeContextParam));
        } else if (baseClass.equals(StaticAsOfJoinStateManagerTypedBase.class)) {
            builder.classPrefix("StaticAsOfJoinHasher").packageGroup("asofjoin").packageMiddle("staticopen")
                    .openAddressedAlternate(false)
                    .stateType(Object.class).mainStateName("rightRowSetSource")
                    .emptyStateName("EMPTY_RIGHT_STATE")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .moveMainFull(TypedAsOfJoinFactory::staticMoveMainFull)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedAsOfJoinFactory::staticRehashSetup);

            final TypeName longArraySource = TypeName.get(LongArraySource.class);
            final ParameterSpec hashSlots = ParameterSpec.builder(longArraySource, "hashSlots").build();
            final ParameterSpec hashSlotOffset = ParameterSpec.builder(MutableInt.class, "hashSlotOffset").build();
            final ParameterSpec foundBuilder = ParameterSpec.builder(RowSetBuilderRandom.class, "foundBuilder").build();

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rightSideSentinel",
                    true, true, TypedAsOfJoinFactory::staticBuildLeftFound,
                    TypedAsOfJoinFactory::staticBuildLeftInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateLeftSide", null,
                    true, TypedAsOfJoinFactory::staticProbeDecorateLeftFound,
                    null, hashSlots, hashSlotOffset, foundBuilder));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "rightSideSentinel",
                    true, true, TypedAsOfJoinFactory::staticBuildRightFound,
                    TypedAsOfJoinFactory::staticBuildRightInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateWithRightSide", null,
                    true, TypedAsOfJoinFactory::staticProbeDecorateRightFound, null));

        } else if (baseClass.equals(RightIncrementalAsOfJoinStateManagerTypedBase.class)) {
            final TypeName longArraySource = TypeName.get(LongArraySource.class);
            final ParameterSpec hashSlots = ParameterSpec.builder(longArraySource, "hashSlots").build();
            final ParameterSpec sequentialBuilders =
                    ParameterSpec.builder(ObjectArraySource.class, "sequentialBuilders").build();

            builder.classPrefix("RightIncrementalAsOfJoinHasher").packageGroup("asofjoin")
                    .packageMiddle("rightincopen")
                    .openAddressedAlternate(true)
                    .stateType(byte.class).mainStateName("stateSource")
                    .overflowOrAlternateStateName("alternateStateSource")
                    .emptyStateName("ENTRY_EMPTY_STATE")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .addExtraPartialRehashParameter(hashSlots)
                    .moveMainFull(TypedAsOfJoinFactory::rightIncrementalMoveMainFull)
                    .moveMainAlternate(TypedAsOfJoinFactory::rightIncrementalMoveMainAlternate)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedAsOfJoinFactory::rightIncrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rowState",
                    true, true, TypedAsOfJoinFactory::rightIncrementalBuildLeftFound,
                    TypedAsOfJoinFactory::rightIncrementalBuildLeftInsert, hashSlots, sequentialBuilders));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "rowState", true,
                    true, TypedAsOfJoinFactory::rightIncrementalRightFound,
                    TypedAsOfJoinFactory::rightIncrementalRightInsert, hashSlots, sequentialBuilders));

            builder.addProbe(new HasherConfig.ProbeSpec("probeRightSide", "rowState",
                    true, TypedAsOfJoinFactory::rightIncrementalProbeDecorateRightFound, null, hashSlots,
                    sequentialBuilders));
        } else if (baseClass.equals(UpdateByStateManagerTypedBase.class)) {
            final ClassName rowKeyType = ClassName.get(RowKeys.class);
            final ParameterizedTypeName chunkType =
                    ParameterizedTypeName.get(ClassName.get(WritableIntChunk.class), rowKeyType);
            final ParameterSpec outputPositions = ParameterSpec.builder(chunkType, "outputPositions").build();
            final ParameterSpec outputPositionOffset =
                    ParameterSpec.builder(MutableInt.class, "outputPositionOffset").build();

            builder.classPrefix("UpdateByHasher").packageGroup("updateby.hashing")
                    .packageMiddle("open")
                    .openAddressedAlternate(true)
                    .stateType(int.class).mainStateName("stateSource")
                    .overflowOrAlternateStateName("alternateStateSource")
                    .emptyStateName("EMPTY_RIGHT_VALUE")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .addExtraPartialRehashParameter(outputPositions)
                    .moveMainFull(TypedUpdateByFactory::incrementalMoveMainFull)
                    .moveMainAlternate(TypedUpdateByFactory::incrementalMoveMainAlternate)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedUpdateByFactory::incrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildHashTable", "rowState",
                    true, true, TypedUpdateByFactory::incrementalBuildLeftFound,
                    TypedUpdateByFactory::incrementalBuildLeftInsert, outputPositionOffset, outputPositions));

            builder.addProbe(new HasherConfig.ProbeSpec("probeHashTable", "rowState",
                    true, TypedUpdateByFactory::incrementalProbeFound, TypedUpdateByFactory::incrementalProbeMissing,
                    outputPositions));
        } else {
            throw new UnsupportedOperationException("Unknown class to make: " + baseClass);
        }

        return builder.build();
    }

    private static <T> void configureAggregation(HasherConfig.Builder<T> builder) {
        builder.packageGroup("by");
        builder.stateType(int.class);
        builder.mainStateName("mainOutputPosition");
        builder.overflowOrAlternateStateName("overflowOutputPosition");
        builder.emptyStateName("EMPTY_OUTPUT_POSITION");
        builder.addExtraMethod(TypedAggregationFactory::createFindPositionForKey);
    }

    /**
     * Produce a hasher for the given base class and column sources.
     *
     * @param hasherConfig the configuration of the class to generate
     * @param tableKeySources the key sources
     * @param tableSize the initial table size
     * @param maximumLoadFactor the maximum load factor of the for the table
     * @param targetLoadFactor the load factor that we will rehash to
     * @param <T> the base class
     * @return an instantiated hasher
     */
    public static <T> T make(HasherConfig<T> hasherConfig, ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] originalKeySources, int tableSize,
            double maximumLoadFactor, double targetLoadFactor) {
        if (USE_PREGENERATED_HASHERS) {
            if (hasherConfig.baseClass.equals(StaticChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticopenagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(IncrementalChunkedOperatorAggregationStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.incagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.incopenagg.gen.TypedHashDispatcher
                        .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(StaticNaturalJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.naturaljoin.typed.staticopen.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(RightIncrementalNaturalJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.naturaljoin.typed.rightincopen.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(IncrementalNaturalJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.naturaljoin.typed.incopen.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(StaticAsOfJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.asofjoin.typed.staticopen.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(RightIncrementalAsOfJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.asofjoin.typed.rightincopen.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(UpdateByStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.updateby.hashing.typed.open.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            }
        }

        final ChunkType[] chunkTypes =
                Arrays.stream(tableKeySources).map(ColumnSource::getChunkType).toArray(ChunkType[]::new);
        final String className = hasherName(hasherConfig, chunkTypes);

        JavaFile javaFile = generateHasher(hasherConfig, chunkTypes, className, Optional.of(Modifier.PUBLIC));

        String[] javaStrings = javaFile.toString().split("\n");
        final String javaString =
                Arrays.stream(javaStrings).filter(s -> !s.startsWith("package ")).collect(Collectors.joining("\n"));

        final Class<?> clazz = CompilerTools.compile(className, javaString,
                "io.deephaven.engine.table.impl.by.typed." + hasherConfig.packageMiddle + ".gen");
        if (!hasherConfig.baseClass.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Generated class is not a " + hasherConfig.baseClass.getCanonicalName());
        }

        final Class<? extends T> castedClass = (Class<? extends T>) clazz;

        T retVal;
        try {
            final Constructor<? extends T> constructor1 =
                    castedClass.getDeclaredConstructor(ColumnSource[].class, ColumnSource[].class, int.class,
                            double.class, double.class);
            retVal = constructor1.newInstance(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                    targetLoadFactor);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                | NoSuchMethodException e) {
            throw new UncheckedDeephavenException("Could not instantiate " + castedClass.getCanonicalName(), e);
        }
        return retVal;
    }

    @NotNull
    public static String packageName(HasherConfig<?> hasherConfig) {
        return "io.deephaven.engine.table.impl." + hasherConfig.packageGroup + ".typed." + hasherConfig.packageMiddle
                + ".gen";
    }

    @NotNull
    public static String hasherName(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        return hasherConfig.classPrefix
                + Arrays.stream(chunkTypes).map(Objects::toString).collect(Collectors.joining(""));
    }

    @NotNull
    public static <T> JavaFile generateHasher(final HasherConfig<T> hasherConfig,
            final ChunkType[] chunkTypes,
            final String className,
            Optional<Modifier> visibility) {
        final String packageName = packageName(hasherConfig);
        final TypeSpec.Builder hasherBuilder =
                TypeSpec.classBuilder(className).addModifiers(Modifier.FINAL).superclass(hasherConfig.baseClass);
        visibility.ifPresent(hasherBuilder::addModifiers);

        hasherBuilder.addMethod(createConstructor(hasherConfig, chunkTypes, hasherBuilder));

        if (hasherConfig.openAddressed) {
            hasherBuilder.addMethod(createNextTableLocationMethod(false));
            if (hasherConfig.openAddressedAlternate) {
                hasherBuilder.addMethod(createNextTableLocationMethod(true));
            }
            hasherConfig.builds.forEach(
                    bs -> hasherBuilder.addMethod(createBuildMethodForOpenAddressed(hasherConfig, bs, chunkTypes)));
            hasherConfig.probes.forEach(
                    ps -> hasherBuilder.addMethod(createProbeMethodForOpenAddressed(hasherConfig, ps, chunkTypes)));
        } else {
            hasherBuilder.addMethod(createBuildMethodForOverflow(hasherConfig, chunkTypes));
            hasherBuilder.addMethod(createProbeMethodForOverflow(hasherConfig, chunkTypes));
        }
        hasherBuilder.addMethod(createHashMethod(chunkTypes));

        if (hasherConfig.openAddressed) {
            if (hasherConfig.openAddressedAlternate) {
                hasherBuilder.addMethod(createMigrateLocationMethod(hasherConfig, chunkTypes));
                hasherBuilder.addMethod(createRehashInternalPartialMethod(hasherConfig, chunkTypes));
                hasherBuilder.addMethod(createNewAlternateMethod(hasherConfig, chunkTypes));
                hasherBuilder.addMethod(createClearAlternateMethod(hasherConfig, chunkTypes));
                hasherBuilder.addMethod(createMigrateFront(hasherConfig));
            }
            if (hasherConfig.supportRehash) {
                hasherBuilder.addMethod(createRehashInternalFullMethod(hasherConfig, chunkTypes));
            }
        } else {
            hasherBuilder.addMethod(createRehashBucketMethod(hasherConfig, chunkTypes));
            hasherBuilder.addMethod(createMaybeMoveMainBucket(hasherConfig, chunkTypes));
        }

        if (!hasherConfig.openAddressed) {
            hasherBuilder.addMethod(createFindOverflow(chunkTypes));
        }
        hasherConfig.extraMethods.forEach(em -> hasherBuilder.addMethod(em.apply(hasherConfig, chunkTypes)));

        final TypeSpec hasher = hasherBuilder.build();

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, hasher).indent("    ");
        fileBuilder.addFileComment("DO NOT EDIT THIS CLASS, AUTOMATICALLY GENERATED BY "
                + TypedHasherFactory.class.getCanonicalName() + "\n" +
                "Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending\n");

        for (ChunkType chunkType : chunkTypes) {
            fileBuilder.addStaticImport(
                    ClassName.get(CharComparisons.class.getPackageName(), chunkType.name() + "Comparisons"), "eq");
        }
        return fileBuilder.build();
    }

    private static MethodSpec createNextTableLocationMethod(boolean alternate) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder(nextTableLocationName(alternate)).addParameter(int.class, "tableLocation")
                        .returns(int.class).addModifiers(Modifier.PRIVATE);
        if (alternate) {
            builder.addStatement("return (tableLocation + 1) & (alternateTableSize - 1)");
        } else {
            builder.addStatement("return (tableLocation + 1) & (tableSize - 1)");
        }
        return builder.build();
    }

    @NotNull
    private static String nextTableLocationName(boolean alternate) {
        return alternate ? "alternateNextTableLocation" : "nextTableLocation";
    }

    @NotNull
    private static <T> MethodSpec createConstructor(HasherConfig<T> hasherConfig, ChunkType[] chunkTypes,
            TypeSpec.Builder hasherBuilder) {
        CodeBlock.Builder constructorCodeBuilder = CodeBlock.builder();
        if (hasherConfig.openAddressed) {
            if (hasherConfig.includeOriginalSources) {
                constructorCodeBuilder
                        .addStatement("super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor)");
            } else {
                constructorCodeBuilder.addStatement("super(tableKeySources, tableSize, maximumLoadFactor)");
            }
        } else {
            constructorCodeBuilder
                    .addStatement("super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor)");
        }
        addKeySourceFields(hasherConfig, chunkTypes, hasherBuilder, constructorCodeBuilder);

        return MethodSpec.constructorBuilder()
                .addParameter(ColumnSource[].class, "tableKeySources")
                .addParameter(ColumnSource[].class, "originalTableKeySources")
                .addParameter(int.class, "tableSize").addParameter(double.class, "maximumLoadFactor")
                .addParameter(double.class, "targetLoadFactor").addModifiers(Modifier.PUBLIC)
                .addCode(constructorCodeBuilder.build())
                .build();
    }

    private static void addKeySourceFields(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            TypeSpec.Builder hasherBuilder,
            CodeBlock.Builder constructorCodeBuilder) {
        final Modifier[] modifiers = hasherConfig.openAddressedAlternate ? new Modifier[] {Modifier.PRIVATE}
                : new Modifier[] {Modifier.PRIVATE, Modifier.FINAL};

        final List<FieldSpec> keySources = new ArrayList<>();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> type =
                    hasherConfig.openAddressed ? flatSourceType(chunkTypes[ii]) : arraySourceType(chunkTypes[ii]);
            keySources.add(
                    FieldSpec.builder(type, "mainKeySource" + ii).addModifiers(modifiers)
                            .build());
            constructorCodeBuilder.addStatement("this.mainKeySource$L = ($T) super.mainKeySources[$L]", ii, type, ii);
            if (hasherConfig.openAddressed) {
                constructorCodeBuilder.addStatement("this.mainKeySource$L.ensureCapacity(tableSize)", ii);
                if (hasherConfig.openAddressedAlternate) {
                    keySources.add(FieldSpec.builder(type, "alternateKeySource" + ii)
                            .addModifiers(modifiers).build());
                }
            } else {
                keySources.add(FieldSpec.builder(type, "overflowKeySource" + ii)
                        .addModifiers(modifiers).build());
                constructorCodeBuilder.addStatement("this.overflowKeySource$L = ($T) super.overflowKeySources[$L]", ii,
                        type, ii);
            }
        }
        keySources.forEach(hasherBuilder::addField);
    }

    @NotNull
    private static MethodSpec createFindOverflow(ChunkType[] chunkTypes) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("findOverflow").addParameter(HashHandler.class, "handler");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addParameter(elementType(chunkTypes[ii]), "k" + ii);
        }

        builder.addParameter(int.class, "chunkPosition").addParameter(int.class, "overflowLocation")
                .returns(boolean.class).addModifiers(Modifier.PRIVATE)
                .beginControlFlow("while (overflowLocation != $T.NULL_INT)", QueryConstants.class)
                .beginControlFlow("if (" + getEqualsStatementOverflow(chunkTypes) + ")")
                .addStatement("handler.doOverflowFound(overflowLocation, chunkPosition)")
                .addStatement("return true")
                .endControlFlow()
                .addStatement("overflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)")
                .endControlFlow()
                .addStatement("return false");
        return builder.build();
    }

    @NotNull
    private static MethodSpec createMaybeMoveMainBucket(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        MethodSpec.Builder builder =
                MethodSpec.methodBuilder("maybeMoveMainBucket").addParameter(HashHandler.class, "handler")
                        .addParameter(int.class, "sourceBucket").addParameter(int.class, "destBucket")
                        .addParameter(int.class, "bucketsToAdd").returns(int.class).addModifiers(Modifier.PRIVATE);

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = mainKeySource$L.getUnsafe(sourceBucket)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int location = hashToTableLocation(tableHashPivot + bucketsToAdd, hash)");
        builder.addStatement("final int mainInsertLocation");
        builder.beginControlFlow("if (location == sourceBucket)");
        builder.addStatement("mainInsertLocation = destBucket");
        builder.addStatement("$L.set(destBucket, $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        builder.nextControlFlow("else");
        builder.addStatement("mainInsertLocation = sourceBucket");
        builder.addStatement("$L.set(destBucket, $L.getUnsafe(sourceBucket))", hasherConfig.mainStateName,
                hasherConfig.mainStateName);
        builder.addStatement("$L.set(sourceBucket, $L)", hasherConfig.mainStateName, hasherConfig.emptyStateName);
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(destBucket, k$L)", ii, ii);
            builder.addStatement("mainKeySource$L.set(sourceBucket, $L)", ii, elementNull(chunkTypes[ii]));
        }
        builder.addStatement("handler.doMoveMain(sourceBucket, destBucket)");
        builder.endControlFlow();
        builder.addStatement("return mainInsertLocation");

        return builder
                .build();
    }

    @NotNull
    private static MethodSpec createRehashBucketMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("final int position = $L.getUnsafe(sourceBucket)", hasherConfig.mainStateName);
        builder.beginControlFlow("if (position == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return");
        builder.endControlFlow();

        builder.addStatement(
                "int mainInsertLocation = maybeMoveMainBucket(handler, sourceBucket, destBucket, bucketsToAdd)");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(sourceBucket)");
        builder.addStatement("mainOverflowLocationSource.set(sourceBucket, QueryConstants.NULL_INT)");
        builder.addStatement("mainOverflowLocationSource.set(destBucket, QueryConstants.NULL_INT)");

        builder.beginControlFlow("while (overflowLocation != QueryConstants.NULL_INT)");
        builder.addStatement(
                "final int nextOverflowLocation = overflowOverflowLocationSource.getUnsafe(overflowLocation)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T overflowKey$L = overflowKeySource$L.getUnsafe(overflowLocation)",
                    elementType(chunkTypes[ii]), ii, ii);
        }
        builder.addStatement("final int overflowHash = hash(" + IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "overflowKey" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement(
                "final int overflowTableLocation = hashToTableLocation(tableHashPivot + bucketsToAdd, overflowHash)");
        builder.beginControlFlow("if (overflowTableLocation == mainInsertLocation)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(mainInsertLocation, overflowKey$L)", ii, ii);
        }
        builder.addStatement("$L.set(mainInsertLocation, $L.getUnsafe(overflowLocation))", hasherConfig.mainStateName,
                hasherConfig.overflowOrAlternateStateName);
        builder.addStatement("handler.doPromoteOverflow(overflowLocation, mainInsertLocation)");
        builder.addStatement("$L.set(overflowLocation, QueryConstants.NULL_INT)",
                hasherConfig.overflowOrAlternateStateName);

        // key source loop
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(overflowLocation, $L)", ii, elementNull(chunkTypes[ii]));
        }

        builder.addStatement("freeOverflowLocation(overflowLocation)");
        builder.addStatement("mainInsertLocation = -1");
        builder.nextControlFlow("else");
        builder.addStatement(
                "final int oldOverflowLocation = mainOverflowLocationSource.getUnsafe(overflowTableLocation)");
        builder.addStatement("mainOverflowLocationSource.set(overflowTableLocation, overflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(overflowLocation, oldOverflowLocation)");
        builder.endControlFlow();
        builder.addStatement("overflowLocation = nextOverflowLocation");
        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashBucket").addParameter(HashHandler.class, "handler")
                .addParameter(int.class, "sourceBucket").addParameter(int.class, "destBucket")
                .addParameter(int.class, "bucketsToAdd").returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    // this rehash internal method is not incremental, which makes us nervous about using it for incremental updates
    // we are also using the old arrays as temporary space, so any moving of objects must be done inline (but the
    // single inheritor we have does not do that, so we are not adding that complexity at this instant)
    @NotNull
    private static MethodSpec createRehashInternalFullMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T[] destKeyArray$L = new $T[tableSize]", elementType(chunkTypes[ii]), ii,
                    elementType(chunkTypes[ii]));
        }
        if (hasherConfig.stateType.isPrimitive()) {
            builder.addStatement("final $T[] destState = new $T[tableSize]", hasherConfig.stateType,
                    hasherConfig.stateType);
        } else {
            builder.addStatement("final Object[] destState = new Object[tableSize]");
        }
        builder.addStatement("$T.fill(destState, $L)", Arrays.class, hasherConfig.emptyStateName);

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("final $T [] originalKeyArray$L = mainKeySource$L.getArray()",
                    elementType(chunkTypes[ii]), ii, ii);
            builder.addStatement("mainKeySource$L.setArray(destKeyArray$L)", ii, ii);
        }
        if (hasherConfig.stateType.isPrimitive()) {
            builder.addStatement("final $T [] originalStateArray = $L.getArray()", hasherConfig.stateType,
                    hasherConfig.mainStateName);
        } else {
            builder.addStatement("final Object [] originalStateArray = (Object[])$L.getArray()",
                    hasherConfig.mainStateName);
        }
        builder.addStatement("$L.setArray(destState)", hasherConfig.mainStateName);

        if (hasherConfig.rehashFullSetup != null) {
            hasherConfig.rehashFullSetup.accept(builder);
        }


        builder.beginControlFlow("for (int sourceBucket = 0; sourceBucket < oldSize; ++sourceBucket)");
        if (hasherConfig.stateType.isPrimitive()) {
            builder.addStatement("final $T currentStateValue = originalStateArray[sourceBucket]",
                    hasherConfig.stateType);
        } else {
            builder.addStatement("final $T currentStateValue = ($T)originalStateArray[sourceBucket]",
                    hasherConfig.stateType, hasherConfig.stateType);
        }
        builder.beginControlFlow("if (currentStateValue == $L)", hasherConfig.emptyStateName);
        builder.addStatement("continue");
        builder.endControlFlow();

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = originalKeyArray$L[sourceBucket]", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int firstDestinationTableLocation = hashToTableLocation(hash)");
        builder.addStatement("int destinationTableLocation = firstDestinationTableLocation");
        builder.beginControlFlow("while (true)");
        builder.beginControlFlow("if (destState[destinationTableLocation] == $L)", hasherConfig.emptyStateName);
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("destKeyArray$L[destinationTableLocation] = k$L", ii, ii);
        }
        builder.addStatement("destState[destinationTableLocation] = originalStateArray[sourceBucket]",
                hasherConfig.mainStateName);
        if (!hasherConfig.alwaysMoveMain) {
            builder.beginControlFlow("if (sourceBucket != destinationTableLocation)");
        }
        hasherConfig.moveMainFull.accept(builder);
        if (!hasherConfig.alwaysMoveMain) {
            builder.endControlFlow();
        }
        builder.addStatement("break");
        builder.endControlFlow();
        builder.addStatement("destinationTableLocation = nextTableLocation(destinationTableLocation)");
        builder.addStatement("$T.neq($L, $S, $L, $S)", Assert.class, "destinationTableLocation",
                "destinationTableLocation",
                "firstDestinationTableLocation", "firstDestinationTableLocation");
        builder.endControlFlow();

        builder.endControlFlow();

        return MethodSpec.methodBuilder("rehashInternalFull")
                .returns(void.class)
                .addParameter(int.class, "oldSize", Modifier.FINAL)
                .addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createRehashInternalPartialMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        // ensure the capacity for everything
        builder.addStatement("int rehashedEntries = 0");
        builder.beginControlFlow("while (rehashPointer > 0 && rehashedEntries < entriesToRehash)");
        final String extraParamNames;
        if (hasherConfig.extraPartialRehashParameters.size() > 0) {
            extraParamNames = ", " + hasherConfig.extraPartialRehashParameters.stream().map(ps -> ps.name)
                    .collect(Collectors.joining(", "));
        } else {
            extraParamNames = "";
        }
        builder.beginControlFlow("if (migrateOneLocation(--rehashPointer" + extraParamNames + "))");
        builder.addStatement("rehashedEntries++");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.addStatement("return rehashedEntries");

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("rehashInternalPartial")
                .returns(int.class).addModifiers(Modifier.PROTECTED).addParameter(int.class, "entriesToRehash")
                .addCode(builder.build())
                .addAnnotation(Override.class);

        hasherConfig.extraPartialRehashParameters.forEach(methodBuilder::addParameter);

        return methodBuilder.build();
    }

    @NotNull
    private static MethodSpec createNewAlternateMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("super.newAlternate()");

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> sourceType = flatSourceType(chunkTypes[ii]);
            builder.addStatement("this.mainKeySource$L = ($T)super.mainKeySources[$L]", ii, sourceType, ii);
            builder.addStatement("this.alternateKeySource$L = ($T)super.alternateKeySources[$L]", ii, sourceType, ii);
        }

        return MethodSpec.methodBuilder("newAlternate")
                .returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createClearAlternateMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("super.clearAlternate()");

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("this.alternateKeySource$L = null", ii);
        }

        return MethodSpec.methodBuilder("clearAlternate")
                .returns(void.class).addModifiers(Modifier.PROTECTED)
                .addCode(builder.build())
                .addAnnotation(Override.class)
                .build();
    }

    @NotNull
    private static MethodSpec createMigrateLocationMethod(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("final $T currentStateValue = $L.getUnsafe(locationToMigrate)", hasherConfig.stateType,
                hasherConfig.overflowOrAlternateStateName);
        builder.beginControlFlow("if (currentStateValue == $L)", hasherConfig.emptyStateName);
        builder.addStatement("return false");
        builder.endControlFlow();

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = alternateKeySource$L.getUnsafe(locationToMigrate)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");

        builder.addStatement("int destinationTableLocation = hashToTableLocation(hash)");

        builder.beginControlFlow("while ($L.getUnsafe(destinationTableLocation) != $L)", hasherConfig.mainStateName,
                hasherConfig.emptyStateName);
        builder.addStatement("destinationTableLocation = nextTableLocation(destinationTableLocation)");
        builder.endControlFlow();

        doRehashMoveAlternateToMain(hasherConfig, chunkTypes, builder, "locationToMigrate", "destinationTableLocation");

        builder.addStatement("return true");

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("migrateOneLocation")
                .returns(boolean.class).addModifiers(Modifier.PRIVATE).addParameter(int.class, "locationToMigrate")
                .addCode(builder.build());

        hasherConfig.extraPartialRehashParameters.forEach(methodBuilder::addParameter);

        return methodBuilder.build();
    }

    @NotNull
    private static MethodSpec createMigrateFront(HasherConfig<?> hasherConfig) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("int location = 0");

        final String extraParamNames;
        if (hasherConfig.extraPartialRehashParameters.size() > 0) {
            extraParamNames = ", " + hasherConfig.extraPartialRehashParameters.stream().map(ps -> ps.name)
                    .collect(Collectors.joining(", "));
        } else {
            extraParamNames = "";
        }

        builder.addStatement("while (migrateOneLocation(location++" + extraParamNames + "))");

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("migrateFront")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .addCode(builder.build());

        hasherConfig.extraPartialRehashParameters.forEach(methodBuilder::addParameter);

        return methodBuilder.build();
    }

    private static void doRehashMoveAlternateToMain(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes,
            CodeBlock.Builder builder, final String sourceLocation, final String destinationLocation) {
        // we need to move the keys, states, and call the move main
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set($L, k$L)", ii, destinationLocation, ii);
            if (chunkTypes[ii] == ChunkType.Object) {
                builder.addStatement("alternateKeySource$L.set($L, null)", ii, sourceLocation);
            }
        }
        builder.addStatement("$L.set($L, currentStateValue)", hasherConfig.mainStateName, destinationLocation);
        hasherConfig.moveMainAlternate.accept(builder);
        builder.addStatement("$L.set($L, $L)", hasherConfig.overflowOrAlternateStateName, sourceLocation,
                hasherConfig.emptyStateName);
    }

    @NotNull
    private static MethodSpec createBuildMethodForOverflow(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if ($L.getUnsafe(tableLocation) == $L)", hasherConfig.mainStateName,
                hasherConfig.emptyStateName);
        builder.addStatement("numEntries++");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set(tableLocation, k$L)", ii, ii);
        }
        builder.addStatement("handler.doMainInsert(tableLocation, chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("final int newOverflowLocation = allocateOverflowLocation()");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("overflowKeySource$L.set(newOverflowLocation, k$L)", ii, ii);
        }
        builder.addStatement("mainOverflowLocationSource.set(tableLocation, newOverflowLocation)");
        builder.addStatement("overflowOverflowLocationSource.set(newOverflowLocation, overflowLocation)");
        builder.addStatement("numEntries++");
        builder.addStatement("handler.doOverflowInsert(newOverflowLocation, chunkPosition)");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();


        return MethodSpec.methodBuilder("build")
                .addParameter(HashHandler.class, "handler")
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                .addAnnotation(Override.class).build();
    }

    @NotNull
    private static MethodSpec createBuildMethodForOpenAddressed(HasherConfig<?> hasherConfig,
            HasherConfig.BuildSpec buildSpec,
            ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        if (!buildSpec.allowAlternates && hasherConfig.openAddressedAlternate) {
            builder.addStatement("$T.eqZero(rehashPointer, \"rehashPointer\")", Assert.class);
        }

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        if (buildSpec.requiresRowKeyChunk) {
            builder.addStatement("final $T rowKeyChunk = rowSequence.asRowKeyChunk()",
                    ParameterizedTypeName.get(LongChunk.class, OrderedRowKeys.class));
        }
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        doBuildSearch(hasherConfig, buildSpec, chunkTypes, builder, false);

        builder.endControlFlow();

        MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(buildSpec.name)
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks");
        for (final ParameterSpec param : buildSpec.params) {
            methodBuilder.addParameter(param);
        }
        return methodBuilder
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                // .addAnnotation(Override.class)
                .build();
    }

    private static void doBuildSearch(HasherConfig<?> hasherConfig, HasherConfig.BuildSpec buildSpec,
            ChunkType[] chunkTypes,
            CodeBlock.Builder builder, boolean alternate) {
        final String tableLocationName = alternate ? "alternateTableLocation" : "tableLocation";
        final String firstTableLocationName = alternate ? "firstAlternateTableLocation" : "firstTableLocation";
        final String tableLocationMethod = alternate ? "hashToTableLocationAlternate" : "hashToTableLocation";

        builder.addStatement("final int $L = $L(hash)", firstTableLocationName, tableLocationMethod);
        builder.addStatement("int $L = $L", tableLocationName, firstTableLocationName);
        if (alternate) {
            builder.beginControlFlow("while ($L < rehashPointer)", tableLocationName);
            builder.addStatement("$L = $L.getUnsafe($L)", buildSpec.stateValueName,
                    hasherConfig.overflowOrAlternateStateName, tableLocationName);
        } else {
            builder.beginControlFlow((hasherConfig.openAddressedAlternate ? "MAIN_SEARCH: " : "") + "while (true)");
            builder.addStatement("$T $L = $L.getUnsafe($L)", hasherConfig.stateType, buildSpec.stateValueName,
                    hasherConfig.mainStateName, tableLocationName);
        }
        builder.beginControlFlow("if ($L == $L)", buildSpec.stateValueName, hasherConfig.emptyStateName);

        if (hasherConfig.openAddressedAlternate && !alternate && buildSpec.allowAlternates) {
            // we might need to do an alternative build here
            doBuildSearch(hasherConfig, buildSpec, chunkTypes, builder, true);
        }

        if (!alternate) {
            builder.addStatement("numEntries++");
            for (int ii = 0; ii < chunkTypes.length; ++ii) {
                builder.addStatement("mainKeySource$L.set($L, k$L)", ii, tableLocationName, ii);
            }
            buildSpec.insert.accept(hasherConfig, builder);
        }
        builder.addStatement("break");
        builder.nextControlFlow("else if ("
                + (alternate ? getEqualsStatementAlternate(chunkTypes) : getEqualsStatement(chunkTypes)) + ")");
        buildSpec.found.accept(hasherConfig, alternate, builder);
        if (alternate) {
            builder.addStatement("break MAIN_SEARCH");
        } else {
            builder.addStatement("break");
        }
        builder.nextControlFlow("else");
        if (alternate) {
            builder.addStatement("$L = alternateNextTableLocation($L)", tableLocationName, tableLocationName);
        } else {
            builder.addStatement("$L = nextTableLocation($L)", tableLocationName, tableLocationName);
        }
        builder.addStatement("$T.neq($L, $S, $L, $S)", Assert.class, tableLocationName, tableLocationName,
                firstTableLocationName, firstTableLocationName);
        builder.endControlFlow();
        builder.endControlFlow();
    }

    private static MethodSpec createProbeMethodForOpenAddressed(HasherConfig<?> hasherConfig, HasherConfig.ProbeSpec ps,
            ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }

        if (ps.requiresRowKeyChunk) {
            builder.addStatement("final $T rowKeyChunk = rowSequence.asRowKeyChunk()",
                    ParameterizedTypeName.get(LongChunk.class, OrderedRowKeys.class));
        }

        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        doProbeSearch(hasherConfig, ps, chunkTypes, builder, false);

        builder.endControlFlow();

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder(ps.name)
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks");

        for (final ParameterSpec param : ps.params) {
            methodBuilder.addParameter(param);
        }

        methodBuilder.returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build());
        // .addAnnotation(Override.class)

        return methodBuilder.build();
    }

    private static void doProbeSearch(HasherConfig<?> hasherConfig, HasherConfig.ProbeSpec ps, ChunkType[] chunkTypes,
            CodeBlock.Builder builder, boolean alternate) {
        final String tableLocationName = alternate ? "alternateTableLocation" : "tableLocation";
        final String firstTableLocationName = alternate ? "firstAlternateTableLocation" : "firstTableLocation";
        final String tableLocationMethod = alternate ? "hashToTableLocationAlternate" : "hashToTableLocation";
        final String foundName = alternate ? "alternateFound" : "found";
        final boolean foundBlockRequired = (hasherConfig.openAddressedAlternate && !alternate) || ps.missing != null;

        builder.addStatement("final int $L = $L(hash)", firstTableLocationName, tableLocationMethod);

        if (foundBlockRequired) {
            builder.addStatement("boolean $L = false", foundName);
        }

        if (alternate) {
            builder.beginControlFlow("if ($L < rehashPointer)", firstTableLocationName);
        }
        builder.addStatement("int $L = $L", tableLocationName, firstTableLocationName);


        if (!alternate && ps.stateValueName != null) {
            builder.addStatement("$T $L", hasherConfig.stateType, ps.stateValueName);
        }

        final String stateSourceName =
                alternate ? hasherConfig.overflowOrAlternateStateName : hasherConfig.mainStateName;
        if (ps.stateValueName == null) {
            builder.beginControlFlow("while ($L.getUnsafe($L) != $L)",
                    stateSourceName, tableLocationName,
                    hasherConfig.emptyStateName);
        } else {
            builder.beginControlFlow("while (($L = $L.getUnsafe($L)) != $L)", ps.stateValueName,
                    stateSourceName, tableLocationName,
                    hasherConfig.emptyStateName);
        }

        builder.beginControlFlow(
                "if (" + (alternate ? getEqualsStatementAlternate(chunkTypes) : getEqualsStatement(chunkTypes)) + ")");
        ps.found.accept(hasherConfig, alternate, builder);
        if (foundBlockRequired) {
            builder.addStatement("$L = true", foundName);
        }
        builder.addStatement("break");
        builder.endControlFlow();
        builder.addStatement("$L = $L($L)", tableLocationName, nextTableLocationName(alternate), tableLocationName);
        builder.addStatement("$T.neq($L, $S, $L, $S)", Assert.class, tableLocationName, tableLocationName,
                firstTableLocationName, firstTableLocationName);
        builder.endControlFlow();
        if (alternate) {
            builder.endControlFlow();
        }

        if (foundBlockRequired) {
            builder.beginControlFlow("if (!$L)", foundName);
            if (hasherConfig.openAddressedAlternate && !alternate) {
                doProbeSearch(hasherConfig, ps, chunkTypes, builder, true);
            } else {
                ps.missing.accept(builder);
            }
            builder.endControlFlow();
        }
    }

    @NotNull
    private static MethodSpec createProbeMethodForOverflow(HasherConfig<?> hasherConfig, ChunkType[] chunkTypes) {
        final CodeBlock.Builder builder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ClassName chunkName =
                    ClassName.get(CharChunk.class.getPackageName(), chunkTypes[ii].name() + "Chunk");
            final ClassName valuesName = ClassName.get(Values.class);
            final ParameterizedTypeName chunkTypeName = chunkTypes[ii] == ChunkType.Object
                    ? ParameterizedTypeName.get(chunkName, ClassName.get(Object.class), valuesName)
                    : ParameterizedTypeName.get(chunkName, valuesName);
            builder.addStatement("final $T keyChunk$L = sourceKeyChunks[$L].as$LChunk()", chunkTypeName, ii, ii,
                    chunkTypes[ii].name());
        }
        builder.addStatement("final int chunkSize = keyChunk0.size()");
        builder.beginControlFlow("for (int chunkPosition = 0; chunkPosition < chunkSize; ++chunkPosition)");
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = keyChunk$L.get(chunkPosition)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");
        builder.addStatement("final int tableLocation = hashToTableLocation(tableHashPivot, hash)");
        builder.beginControlFlow("if ($L.getUnsafe(tableLocation) == $L)", hasherConfig.mainStateName,
                hasherConfig.emptyStateName);
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.nextControlFlow("else if (" + getEqualsStatement(chunkTypes) + ")");
        builder.addStatement("handler.doMainFound(tableLocation, chunkPosition)");
        builder.nextControlFlow("else");
        builder.addStatement("int overflowLocation = mainOverflowLocationSource.getUnsafe(tableLocation)");
        builder.beginControlFlow("if (!findOverflow(handler, "
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", "))
                + ", chunkPosition, overflowLocation))");
        builder.addStatement("handler.doMissing(chunkPosition)");
        builder.endControlFlow();
        builder.endControlFlow();
        builder.endControlFlow();

        return MethodSpec.methodBuilder("probe")
                .addParameter(HashHandler.class, "handler")
                .addParameter(RowSequence.class, "rowSequence")
                .addParameter(Chunk[].class, "sourceKeyChunks")
                .returns(void.class).addModifiers(Modifier.PROTECTED).addCode(builder.build())
                .addAnnotation(Override.class).build();
    }



    @NotNull
    private static MethodSpec createHashMethod(ChunkType[] chunkTypes) {
        final MethodSpec.Builder builder =
                MethodSpec.methodBuilder("hash").returns(int.class).addModifiers(Modifier.PRIVATE, Modifier.STATIC);
        final CodeBlock.Builder hashCodeBuilder = CodeBlock.builder();
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final ChunkType chunkType = chunkTypes[ii];
            builder.addParameter(elementType(chunkType), "k" + ii);
            if (ii == 0) {
                hashCodeBuilder.addStatement("int hash = $T.hashInitialSingle(k0)", chunkHasherByChunkType(chunkType));
            } else {
                hashCodeBuilder.addStatement("hash = $T.hashUpdateSingle(hash, k" + ii + ")",
                        chunkHasherByChunkType(chunkType));
            }
        }
        hashCodeBuilder.addStatement("return hash");
        return builder.addCode(hashCodeBuilder.build()).build();
    }

    @NotNull
    public static String getEqualsStatement(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(mainKeySource" + x + ".getUnsafe(tableLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    @NotNull
    public static String getEqualsStatementAlternate(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(alternateKeySource" + x + ".getUnsafe(alternateTableLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    @NotNull
    public static String getEqualsStatementOverflow(ChunkType[] chunkTypes) {
        return IntStream.range(0, chunkTypes.length)
                .mapToObj(x -> "eq(overflowKeySource" + x + ".getUnsafe(overflowLocation), k" + x + ")")
                .collect(Collectors.joining(" && "));
    }

    private static ClassName chunkHasherByChunkType(ChunkType chunkType) {
        return ClassName.get(CharChunkHasher.class.getPackageName(), chunkType.name() + "ChunkHasher");
    }

    static Class<? extends ArrayBackedColumnSource> arraySourceType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return CharacterArraySource.class;
            case Byte:
                return ByteArraySource.class;
            case Short:
                return ShortArraySource.class;
            case Int:
                return IntegerArraySource.class;
            case Long:
                return LongArraySource.class;
            case Float:
                return FloatArraySource.class;
            case Double:
                return DoubleArraySource.class;
            case Object:
                return ObjectArraySource.class;
        }
    }

    static Class<? extends ColumnSource> flatSourceType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return ImmutableCharArraySource.class;
            case Byte:
                return ImmutableByteArraySource.class;
            case Short:
                return ImmutableShortArraySource.class;
            case Int:
                return ImmutableIntArraySource.class;
            case Long:
                return ImmutableLongArraySource.class;
            case Float:
                return ImmutableFloatArraySource.class;
            case Double:
                return ImmutableDoubleArraySource.class;
            case Object:
                return ImmutableObjectArraySource.class;
        }
    }

    static Class<?> elementType(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
                return char.class;
            case Byte:
                return byte.class;
            case Short:
                return short.class;
            case Int:
                return int.class;
            case Long:
                return long.class;
            case Float:
                return float.class;
            case Double:
                return double.class;
            case Object:
                return Object.class;
        }
    }

    private static String elementNull(ChunkType chunkType) {
        switch (chunkType) {
            default:
            case Boolean:
                throw new IllegalArgumentException();
            case Char:
            case Byte:
            case Short:
            case Int:
            case Long:
            case Float:
            case Double:
                return "QueryConstants.NULL_" + chunkType.name().toUpperCase();
            case Object:
                return "null";
        }
    }
}
