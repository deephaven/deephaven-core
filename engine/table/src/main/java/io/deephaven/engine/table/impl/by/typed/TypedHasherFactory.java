//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.typed;

import com.squareup.javapoet.*;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.hashing.CharChunkHasher;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryCompilerRequest;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MultiJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.NaturalJoinModifiedSlotTracker;
import io.deephaven.engine.table.impl.asofjoin.RightIncrementalAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.StaticAsOfJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.asofjoin.TypedAsOfJoinFactory;
import io.deephaven.engine.table.impl.by.*;
import io.deephaven.engine.table.impl.multijoin.IncrementalMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.multijoin.StaticMultiJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.multijoin.TypedMultiJoinFactory;
import io.deephaven.engine.table.impl.naturaljoin.IncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.RightIncrementalNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.StaticNaturalJoinStateManagerTypedBase;
import io.deephaven.engine.table.impl.naturaljoin.TypedNaturalJoinFactory;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.engine.table.impl.sources.immutable.*;
import io.deephaven.engine.table.impl.updateby.hashing.TypedUpdateByFactory;
import io.deephaven.engine.table.impl.updateby.hashing.UpdateByStateManagerTypedBase;
import io.deephaven.util.compare.CharComparisons;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

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
     * @param baseClass the base class (e.g. {@link IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase}
     *        that the generated hasher extends from
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

        if (baseClass.equals(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
            configureAggregation(builder);
            builder.classPrefix("StaticAggOpenHasher").packageMiddle("staticopenagg");
            builder.openAddressedAlternate(false);
            builder.moveMainFull(TypedAggregationFactory::staticAggMoveMain);
            builder.addBuild(
                    new HasherConfig.BuildSpec("build", "outputPosition", false, true,
                            true, TypedAggregationFactory::buildFound,
                            TypedAggregationFactory::buildInsert));
        } else if (baseClass.equals(IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
            configureAggregation(builder);
            builder.classPrefix("IncrementalAggOpenHasher").packageMiddle("incopenagg");
            builder.overflowOrAlternateStateName("alternateOutputPosition");
            builder.moveMainFull(TypedAggregationFactory::incAggMoveMain);
            builder.moveMainAlternate(TypedAggregationFactory::incAggMoveMain);
            builder.alwaysMoveMain(true);

            builder.addProbe(new HasherConfig.ProbeSpec("probe", "outputPosition", false,
                    TypedAggregationFactory::probeFound, TypedAggregationFactory::probeMissing));

            builder.addBuild(new HasherConfig.BuildSpec("build", "outputPosition", false, true,
                    true, TypedAggregationFactory::buildFound, TypedAggregationFactory::buildInsertIncremental));
        } else if (baseClass.equals(StaticNaturalJoinStateManagerTypedBase.class)) {
            builder.classPrefix("StaticNaturalJoinHasher").packageGroup("naturaljoin").packageMiddle("staticopen")
                    .openAddressedAlternate(false)
                    .stateType(long.class).mainStateName("mainRightRowKey")
                    .emptyStateName("EMPTY_RIGHT_STATE")
                    .includeOriginalSources(true)
                    .supportRehash(false);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rightSideSentinel",
                    false, true, true, TypedNaturalJoinFactory::staticBuildLeftFound,
                    TypedNaturalJoinFactory::staticBuildLeftInsert,
                    ParameterSpec.builder(TypeName.get(IntegerArraySource.class), "leftHashSlots").build(),
                    ParameterSpec.builder(long.class, "hashSlotOffset").build()));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateLeftSide", "rightRowKey",
                    false, TypedNaturalJoinFactory::staticProbeDecorateLeftFound,
                    TypedNaturalJoinFactory::staticProbeDecorateLeftMissing,
                    ParameterSpec.builder(TypeName.get(LongArraySource.class), "leftRedirections").build(),
                    ParameterSpec.builder(long.class, "redirectionOffset").build()));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "rightSideSentinel",
                    true, true, true, TypedNaturalJoinFactory::staticBuildRightFound,
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
                    true, true, true, TypedNaturalJoinFactory::rightIncrementalBuildLeftFound,
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
                    .supportTombstones(true)
                    .stateType(long.class).mainStateName("mainRightRowKey")
                    .overflowOrAlternateStateName("alternateRightRowKey")
                    .emptyStateName("EMPTY_RIGHT_STATE")
                    .tombstoneStateName("TOMBSTONE_RIGHT_STATE")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .addExtraPartialRehashParameter(modifiedSlotTrackerParam)
                    .moveMainFull(TypedNaturalJoinFactory::incrementalMoveMainFull)
                    .moveMainAlternate(TypedNaturalJoinFactory::incrementalMoveMainAlternate)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedNaturalJoinFactory::incrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rightRowKeyForState",
                    true, false, false, TypedNaturalJoinFactory::incrementalBuildLeftFound,
                    TypedNaturalJoinFactory::incrementalBuildLeftInsert));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "existingRightRowKey", true,
                    false, false, TypedNaturalJoinFactory::incrementalRightFound,
                    TypedNaturalJoinFactory::incrementalRightInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("removeRight", "existingRightRowKey", true,
                    TypedNaturalJoinFactory::incrementalRemoveRightFound,
                    TypedNaturalJoinFactory::incrementalRemoveRightMissing,
                    modifiedSlotTrackerParam));

            builder.addBuild(new HasherConfig.BuildSpec("addRightSide", "existingRightRowKey", true,
                    true, true, TypedNaturalJoinFactory::incrementalRightFoundUpdate,
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
                    true, true, TypedNaturalJoinFactory::incrementalLeftFoundUpdate,
                    TypedNaturalJoinFactory::incrementalLeftInsertUpdate,
                    ParameterSpec.builder(TypeName.get(LongArraySource.class), "leftRedirections").build(),
                    ParameterSpec.builder(long.class, "leftRedirectionOffset").build()));

            builder.addProbe(new HasherConfig.ProbeSpec("removeLeft", "rightState", true,
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

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rightSideSentinel",
                    true, true, true, TypedAsOfJoinFactory::staticBuildLeftFound,
                    TypedAsOfJoinFactory::staticBuildLeftInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateLeftSide", null, true,
                    TypedAsOfJoinFactory::staticProbeDecorateLeftFound, null,
                    ParameterSpec.builder(TypeName.get(IntegerArraySource.class), "hashSlots").build(),
                    ParameterSpec.builder(MutableInt.class, "hashSlotOffset").build(),
                    ParameterSpec.builder(RowSetBuilderRandom.class, "foundBuilder").build()));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "rightSideSentinel",
                    true, true, true, TypedAsOfJoinFactory::staticBuildRightFound,
                    TypedAsOfJoinFactory::staticBuildRightInsert));

            builder.addProbe(new HasherConfig.ProbeSpec("decorateWithRightSide", null,
                    true, TypedAsOfJoinFactory::staticProbeDecorateRightFound, null));

        } else if (baseClass.equals(RightIncrementalAsOfJoinStateManagerTypedBase.class)) {
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
                    .moveMainFull(TypedAsOfJoinFactory::rightIncrementalMoveMainFull)
                    .moveMainAlternate(TypedAsOfJoinFactory::rightIncrementalMoveMainAlternate)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedAsOfJoinFactory::rightIncrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromLeftSide", "rowState",
                    true, true, true, TypedAsOfJoinFactory::rightIncrementalBuildLeftFound,
                    TypedAsOfJoinFactory::rightIncrementalBuildLeftInsert, sequentialBuilders));

            builder.addBuild(new HasherConfig.BuildSpec("buildFromRightSide", "rowState", true,
                    true, true, TypedAsOfJoinFactory::rightIncrementalRightFound,
                    TypedAsOfJoinFactory::rightIncrementalRightInsert, sequentialBuilders));

            builder.addProbe(new HasherConfig.ProbeSpec("probeRightSide", "rowState",
                    true, TypedAsOfJoinFactory::rightIncrementalProbeDecorateRightFound, null,
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
                    true, true, true, TypedUpdateByFactory::incrementalBuildLeftFound,
                    TypedUpdateByFactory::incrementalBuildLeftInsert, outputPositionOffset, outputPositions));

            builder.addProbe(new HasherConfig.ProbeSpec("probeHashTable", "rowState",
                    true, TypedUpdateByFactory::incrementalProbeFound, TypedUpdateByFactory::incrementalProbeMissing,
                    outputPositions));
        } else if (baseClass.equals(StaticMultiJoinStateManagerTypedBase.class)) {
            builder.classPrefix("StaticMultiJoinHasher").packageGroup("multijoin").packageMiddle("staticopen")
                    .openAddressedAlternate(false)
                    .stateType(int.class).mainStateName("slotToOutputRow")
                    .emptyStateName("EMPTY_OUTPUT_ROW")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .moveMainFull(TypedMultiJoinFactory::staticMoveMainFull)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedMultiJoinFactory::staticRehashSetup);


            builder.addBuild(new HasherConfig.BuildSpec("buildFromTable", "slotValue",
                    true, true, true, TypedMultiJoinFactory::staticBuildLeftFound,
                    TypedMultiJoinFactory::staticBuildLeftInsert,
                    ParameterSpec.builder(TypeName.get(LongArraySource.class), "tableRedirSource").build(),
                    ParameterSpec.builder(long.class, "tableNumber").build()));
        } else if (baseClass.equals(IncrementalMultiJoinStateManagerTypedBase.class)) {
            final ParameterSpec modifiedSlotTrackerParam =
                    ParameterSpec.builder(MultiJoinModifiedSlotTracker.class, "modifiedSlotTracker").build();
            final ParameterSpec tableRedirSourceParam =
                    ParameterSpec.builder(TypeName.get(LongArraySource.class), "tableRedirSource").build();
            final ParameterSpec tableNumberParam =
                    ParameterSpec.builder(int.class, "tableNumber").build();
            final ParameterSpec flagParam =
                    ParameterSpec.builder(byte.class, "trackerFlag").build();

            builder.classPrefix("IncrementalMultiJoinHasher").packageGroup("multijoin")
                    .packageMiddle("incopen")
                    .openAddressedAlternate(true)
                    .stateType(int.class).mainStateName("slotToOutputRow")
                    .overflowOrAlternateStateName("alternateSlotToOutputRow")
                    .emptyStateName("EMPTY_OUTPUT_ROW")
                    .includeOriginalSources(true)
                    .supportRehash(true)
                    .moveMainFull(TypedMultiJoinFactory::incrementalMoveMainFull)
                    .moveMainAlternate(TypedMultiJoinFactory::incrementalMoveMainAlternate)
                    .alwaysMoveMain(true)
                    .rehashFullSetup(TypedMultiJoinFactory::incrementalRehashSetup);

            builder.addBuild(new HasherConfig.BuildSpec("buildFromTable", "slotValue",
                    true, true,
                    true, TypedMultiJoinFactory::incrementalBuildLeftFound,
                    TypedMultiJoinFactory::incrementalBuildLeftInsert,
                    tableRedirSourceParam,
                    tableNumberParam,
                    modifiedSlotTrackerParam,
                    flagParam));

            builder.addProbe(new HasherConfig.ProbeSpec("remove", "slotValue", true,
                    TypedMultiJoinFactory::incrementalRemoveLeftFound,
                    TypedMultiJoinFactory::incrementalRemoveLeftMissing,
                    tableRedirSourceParam,
                    tableNumberParam,
                    modifiedSlotTrackerParam,
                    flagParam));

            builder.addProbe(new HasherConfig.ProbeSpec("shift", "slotValue", true,
                    TypedMultiJoinFactory::incrementalShiftLeftFound,
                    TypedMultiJoinFactory::incrementalShiftLeftMissing,
                    tableRedirSourceParam,
                    tableNumberParam,
                    modifiedSlotTrackerParam,
                    flagParam,
                    ParameterSpec.builder(long.class, "shiftDelta").build()));

            builder.addProbe(new HasherConfig.ProbeSpec("modify", "slotValue", false,
                    TypedMultiJoinFactory::incrementalModifyLeftFound,
                    TypedMultiJoinFactory::incrementalModifyLeftMissing,
                    tableRedirSourceParam,
                    tableNumberParam,
                    modifiedSlotTrackerParam,
                    flagParam));
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
            if (hasherConfig.baseClass
                    .equals(StaticChunkedOperatorAggregationStateManagerOpenAddressedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher = (T) io.deephaven.engine.table.impl.by.typed.staticopenagg.gen.TypedHashDispatcher
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
            } else if (hasherConfig.baseClass
                    .equals(StaticMultiJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.multijoin.typed.staticopen.gen.TypedHashDispatcher
                                .dispatch(tableKeySources, originalKeySources, tableSize, maximumLoadFactor,
                                        targetLoadFactor);
                if (pregeneratedHasher != null) {
                    return pregeneratedHasher;
                }
            } else if (hasherConfig.baseClass
                    .equals(IncrementalMultiJoinStateManagerTypedBase.class)) {
                // noinspection unchecked
                T pregeneratedHasher =
                        (T) io.deephaven.engine.table.impl.multijoin.typed.incopen.gen.TypedHashDispatcher
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

        final Class<?> clazz = ExecutionContext.getContext().getQueryCompiler().compile(QueryCompilerRequest.builder()
                .description("TypedHasherFactory: " + className)
                .className(className)
                .classBody(javaString)
                .packageNameRoot("io.deephaven.engine.table.impl.by.typed." + hasherConfig.packageMiddle + ".gen")
                .build());

        if (!hasherConfig.baseClass.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Generated class is not a " + hasherConfig.baseClass.getCanonicalName());
        }

        // noinspection unchecked
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
            final Optional<Modifier> visibility) {
        final String packageName = packageName(hasherConfig);
        final TypeSpec.Builder hasherBuilder =
                TypeSpec.classBuilder(className).addModifiers(Modifier.FINAL).superclass(hasherConfig.baseClass);
        visibility.ifPresent(hasherBuilder::addModifiers);

        hasherBuilder.addMethod(createConstructor(hasherConfig, chunkTypes, hasherBuilder));

        hasherBuilder.addMethod(createNextTableLocationMethod(false));
        if (hasherConfig.openAddressedAlternate) {
            hasherBuilder.addMethod(createNextTableLocationMethod(true));
        }
        hasherConfig.builds.forEach(
                bs -> hasherBuilder.addMethod(createBuildMethodForOpenAddressed(hasherConfig, bs, chunkTypes)));
        hasherConfig.probes.forEach(
                ps -> hasherBuilder.addMethod(createProbeMethodForOpenAddressed(hasherConfig, ps, chunkTypes)));
        hasherBuilder.addMethod(createHashMethod(chunkTypes));

        hasherBuilder.addMethod(createIsEmptyMethod(hasherConfig));
        if (hasherConfig.supportTombstones) {
            hasherBuilder.addMethod(createIsDeletedMethod(hasherConfig));
        }
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

        hasherConfig.extraMethods.forEach(em -> hasherBuilder.addMethod(em.apply(hasherConfig, chunkTypes)));

        final TypeSpec hasher = hasherBuilder.build();

        final JavaFile.Builder fileBuilder = JavaFile.builder(packageName, hasher).indent("    ");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY\n");
        fileBuilder
                .addFileComment("****** Run ReplicateTypedHashers or ./gradlew replicateTypedHashers to regenerate\n");
        fileBuilder.addFileComment("\n");
        fileBuilder.addFileComment("@formatter:off");

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
        if (hasherConfig.includeOriginalSources) {
            constructorCodeBuilder
                    .addStatement("super(tableKeySources, originalTableKeySources, tableSize, maximumLoadFactor)");
        } else {
            constructorCodeBuilder.addStatement("super(tableKeySources, tableSize, maximumLoadFactor)");
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
            final Class<?> type = flatSourceType(chunkTypes[ii]);
            keySources.add(
                    FieldSpec.builder(type, "mainKeySource" + ii).addModifiers(modifiers)
                            .build());
            constructorCodeBuilder.addStatement("this.mainKeySource$L = ($T) super.mainKeySources[$L]", ii, type, ii);
            constructorCodeBuilder.addStatement("this.mainKeySource$L.ensureCapacity(tableSize)", ii);
            if (hasherConfig.openAddressedAlternate) {
                keySources.add(FieldSpec.builder(type, "alternateKeySource" + ii)
                        .addModifiers(modifiers).build());
            }
        }
        keySources.forEach(hasherBuilder::addField);
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
        builder.beginControlFlow("if (isStateEmpty(currentStateValue))");
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
        if (hasherConfig.stateType.isPrimitive()) {
            builder.beginControlFlow("if (isStateEmpty(destState[destinationTableLocation]))");
        } else {
            builder.beginControlFlow("if (isStateEmpty(($T)destState[destinationTableLocation]))",
                    hasherConfig.stateType);
        }
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
        final String extraParamNames = getExtraMigrateParams(hasherConfig.extraPartialRehashParameters);
        final String deletedParam = hasherConfig.supportTombstones ? ", false" : "";

        builder.beginControlFlow("if (migrateOneLocation(--rehashPointer" + deletedParam + extraParamNames + "))");
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

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> sourceType = flatSourceType(chunkTypes[ii]);
            builder.addStatement("this.mainKeySource$L = ($T)super.mainKeySources[$L]", ii, sourceType, ii);
            builder.addStatement("this.alternateKeySource$L = ($T)super.alternateKeySources[$L]", ii, sourceType, ii);
        }

        return MethodSpec.methodBuilder("adviseNewAlternate")
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
        builder.beginControlFlow("if (isStateEmpty(currentStateValue))");
        builder.addStatement("return false");
        builder.endControlFlow();
        if (hasherConfig.supportTombstones) {
            builder.beginControlFlow("if (isStateDeleted(currentStateValue))");
            builder.addStatement("alternateEntries--");
            builder.addStatement("$L.set(locationToMigrate, $L)", hasherConfig.overflowOrAlternateStateName,
                    hasherConfig.emptyStateName);
            builder.addStatement("return trueOnDeletedEntry");
            builder.endControlFlow();
        }

        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            final Class<?> element = elementType(chunkTypes[ii]);
            builder.addStatement("final $T k$L = alternateKeySource$L.getUnsafe(locationToMigrate)", element, ii, ii);
        }
        builder.addStatement("final int hash = hash("
                + IntStream.range(0, chunkTypes.length).mapToObj(x -> "k" + x).collect(Collectors.joining(", ")) + ")");

        builder.addStatement("int destinationTableLocation = hashToTableLocation(hash)");

        if (hasherConfig.supportTombstones) {
            builder.addStatement("$T candidateState", hasherConfig.stateType);
            builder.beginControlFlow(
                    "while (!isStateEmpty(candidateState = $L.getUnsafe(destinationTableLocation)) && !isStateDeleted(candidateState))",
                    hasherConfig.mainStateName);
        } else {
            builder.beginControlFlow("while (!isStateEmpty($L.getUnsafe(destinationTableLocation)))",
                    hasherConfig.mainStateName);
        }
        builder.addStatement("destinationTableLocation = nextTableLocation(destinationTableLocation)");
        builder.endControlFlow();

        doRehashMoveAlternateToMain(hasherConfig, chunkTypes, builder, "locationToMigrate", "destinationTableLocation");

        builder.addStatement("return true");

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("migrateOneLocation")
                .returns(boolean.class).addModifiers(Modifier.PRIVATE)
                .addParameter(int.class, "locationToMigrate")
                .addCode(builder.build());

        if (hasherConfig.supportTombstones) {
            methodBuilder.addParameter(boolean.class, "trueOnDeletedEntry");
        }

        hasherConfig.extraPartialRehashParameters.forEach(methodBuilder::addParameter);

        return methodBuilder.build();
    }

    @NotNull
    private static MethodSpec createMigrateFront(HasherConfig<?> hasherConfig) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("int location = 0");

        final String extraParamNames = getExtraMigrateParams(hasherConfig.extraPartialRehashParameters);

        final String deletedParam = hasherConfig.supportTombstones ? ", true" : "";

        builder.addStatement(
                "while (migrateOneLocation(location++" + deletedParam + extraParamNames
                        + ") && location < alternateTableSize)");

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("migrateFront")
                .addModifiers(Modifier.PROTECTED)
                .addAnnotation(Override.class)
                .addCode(builder.build());

        hasherConfig.extraPartialRehashParameters.forEach(methodBuilder::addParameter);

        return methodBuilder.build();
    }

    private static @NotNull String getExtraMigrateParams(List<ParameterSpec> hasherConfig) {
        final String extraParamNames;
        if (!hasherConfig.isEmpty()) {
            extraParamNames = ", " + hasherConfig.stream().map(ps -> ps.name)
                    .collect(Collectors.joining(", "));
        } else {
            extraParamNames = "";
        }
        return extraParamNames;
    }

    /**
     * The isStateEmpty method indicates that the state is empty, for probe iteration.
     */
    private static MethodSpec createIsEmptyMethod(HasherConfig<?> hasherConfig) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("return state == $L", hasherConfig.emptyStateName);

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("isStateEmpty")
                .addModifiers(Modifier.STATIC, Modifier.PRIVATE)
                .returns(boolean.class)
                .addParameter(hasherConfig.stateType, "state")
                .addCode(builder.build());

        return methodBuilder.build();
    }

    /**
     * The isStateDeleted method indicates that the state is deleted, for probe iteration.
     */
    private static MethodSpec createIsDeletedMethod(HasherConfig<?> hasherConfig) {
        final CodeBlock.Builder builder = CodeBlock.builder();

        builder.addStatement("return state == $L", hasherConfig.tombstoneStateName);

        final MethodSpec.Builder methodBuilder = MethodSpec.methodBuilder("isStateDeleted")
                .addModifiers(Modifier.STATIC, Modifier.PRIVATE)
                .returns(boolean.class)
                .addParameter(hasherConfig.stateType, "state")
                .addCode(builder.build());

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
        // with tombstone support, we must track the entries and the alternateEntriest separately
        if (hasherConfig.supportTombstones) {
            builder.beginControlFlow("if (!isStateDeleted(candidateState))");
            builder.addStatement("numEntries++");
            builder.endControlFlow();
            builder.addStatement("alternateEntries--");
        }
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
            if (hasherConfig.supportTombstones && buildSpec.checkDeletions) {
                builder.addStatement("int firstDeletedLocation = -1");
            }
            builder.beginControlFlow((hasherConfig.openAddressedAlternate ? "MAIN_SEARCH: " : "") + "while (true)");
            builder.addStatement("$T $L = $L.getUnsafe($L)", hasherConfig.stateType, buildSpec.stateValueName,
                    hasherConfig.mainStateName, tableLocationName);
        }

        if (!alternate && hasherConfig.supportTombstones && buildSpec.checkDeletions) {
            builder.beginControlFlow("if (firstDeletedLocation < 0 && isStateDeleted($L))", buildSpec.stateValueName);
            builder.addStatement("firstDeletedLocation = $L", tableLocationName);
            builder.endControlFlow();
        }

        builder.beginControlFlow("if (isStateEmpty($L))", buildSpec.stateValueName);

        if (hasherConfig.openAddressedAlternate && !alternate && buildSpec.allowAlternates) {
            // we might need to do an alternative build here
            doBuildSearch(hasherConfig, buildSpec, chunkTypes, builder, true);
        }

        if (!alternate) {
            doInsertion(hasherConfig, buildSpec, chunkTypes, builder, tableLocationName, true);
        }
        builder.addStatement("break");
        final String keyEqualsExpression =
                alternate ? getEqualsStatementAlternate(chunkTypes) : getEqualsStatement(chunkTypes);
        builder.nextControlFlow("else if (" + keyEqualsExpression + ")");
        if (hasherConfig.supportTombstones && buildSpec.checkDeletions) {
            builder.beginControlFlow("if (isStateDeleted($L))", buildSpec.stateValueName);
            // we can terminate our probe here, and insert into the firstDeleted location (or here)
            if (!alternate) {
                doInsertion(hasherConfig, buildSpec, chunkTypes, builder, tableLocationName, false);
            }
            builder.addStatement("break");
            builder.endControlFlow();
        }
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

    /**
     * @param hasherConfig the hasher spec
     * @param buildSpec the spec for the specific builder
     * @param chunkTypes the types of our chunks
     * @param builder the method builder we are adding to
     * @param tableLocationName the name of the table location
     * @param checkForDeletedLocation true if we need to check the firstDeletedLocation variable, false if are sure that
     *        it is set and we do not need to check it
     */
    private static void doInsertion(HasherConfig<?> hasherConfig,
            HasherConfig.BuildSpec buildSpec,
            ChunkType[] chunkTypes,
            CodeBlock.Builder builder,
            String tableLocationName,
            boolean checkForDeletedLocation) {
        if (hasherConfig.supportTombstones) {
            // set to the first available location
            if (buildSpec.checkDeletions) {
                if (checkForDeletedLocation) {
                    builder.beginControlFlow("if (firstDeletedLocation >= 0)");
                }
                builder.addStatement("tableLocation = firstDeletedLocation");
                if (checkForDeletedLocation) {
                    builder.nextControlFlow("else");
                    builder.addStatement("numEntries++");
                    builder.endControlFlow();
                }
            } else {
                builder.addStatement("numEntries++");
            }
            builder.addStatement("liveEntries++");
        } else {
            builder.addStatement("numEntries++");
        }
        doInsertCommon(hasherConfig, buildSpec, chunkTypes, builder, tableLocationName);
    }

    private static void doInsertCommon(HasherConfig<?> hasherConfig, HasherConfig.BuildSpec buildSpec,
            ChunkType[] chunkTypes, CodeBlock.Builder builder, String tableLocationName) {
        for (int ii = 0; ii < chunkTypes.length; ++ii) {
            builder.addStatement("mainKeySource$L.set($L, k$L)", ii, tableLocationName, ii);
        }
        buildSpec.insert.accept(hasherConfig, chunkTypes, builder);
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
        if (hasherConfig.supportTombstones && !alternate) {
            builder.addStatement("boolean searchAlternate = true");
        }

        if (alternate) {
            builder.beginControlFlow("if ($L < rehashPointer)", firstTableLocationName);
        }
        builder.addStatement("int $L = $L", tableLocationName, firstTableLocationName);

        final String stateValueName;
        if (ps.stateValueName != null) {
            stateValueName = ps.stateValueName;
        } else if (hasherConfig.supportTombstones) {
            stateValueName = "stateValue";
        } else {
            stateValueName = null;
        }
        if (!alternate && stateValueName != null) {
            builder.addStatement("$T $L", hasherConfig.stateType, stateValueName);
        }

        final String stateSourceName =
                alternate ? hasherConfig.overflowOrAlternateStateName : hasherConfig.mainStateName;
        if (stateValueName == null) {
            builder.beginControlFlow("while (!isStateEmpty($L.getUnsafe($L)))",
                    stateSourceName, tableLocationName);
        } else {
            builder.beginControlFlow("while (!isStateEmpty($L = $L.getUnsafe($L)))", stateValueName,
                    stateSourceName, tableLocationName);
        }

        final String equalsStatement =
                alternate ? getEqualsStatementAlternate(chunkTypes) : getEqualsStatement(chunkTypes);
        builder.beginControlFlow("if (" + equalsStatement + ")");
        if (hasherConfig.supportTombstones) {
            builder.beginControlFlow("if (isStateDeleted($L))", stateValueName);
            if (!alternate) {
                // never going to find it now
                builder.addStatement("searchAlternate = false");
            }
            builder.addStatement("break");
            builder.endControlFlow();
        }
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
            if (hasherConfig.supportTombstones && !alternate) {
                builder.beginControlFlow("if (!searchAlternate)");
                ps.missing.accept(builder);
                builder.nextControlFlow("else");
            }
            if (hasherConfig.openAddressedAlternate && !alternate) {
                doProbeSearch(hasherConfig, ps, chunkTypes, builder, true);
            } else {
                ps.missing.accept(builder);
            }
            if (hasherConfig.supportTombstones && !alternate) {
                builder.endControlFlow();
            }
            builder.endControlFlow();
        }
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

    private static ClassName chunkHasherByChunkType(ChunkType chunkType) {
        return ClassName.get(CharChunkHasher.class.getPackageName(), chunkType.name() + "ChunkHasher");
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
}
