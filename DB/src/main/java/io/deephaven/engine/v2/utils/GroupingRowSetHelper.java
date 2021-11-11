/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.tuple.EmptyTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.LogicalClock;
import io.deephaven.engine.tuplesource.TupleSource;
import io.deephaven.engine.tuplesource.TupleSourceFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Intermediate base class to separate grouping functionality from tracking functionality to be exposed in
 * {@link TrackingMutableRowSetImpl}.
 */
public abstract class GroupingRowSetHelper extends MutableRowSetImpl implements TrackingMutableRowSet {

    /*
     * TODO (https://github.com/deephaven/deephaven-core/issues/1521): We need to do something better than
     * weakly-reachable List<ColumnSource>s here. Indices are probably cleaned up well before we want right now. Values
     * are only cleaned up on access. Both are sub-par. Worse, the keys are often strongly reachable from the values.
     */

    /**
     * These mappings last forever, and only include column sources that are immutable. Whenever this RowSet is changed,
     * we must clear the mappings.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> mappings = null;

    /**
     * These mappings do not survive past a single LogicalClock tick or any RowSet changes.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings = null;

    /**
     * These prev mappings do not survive past a single LogicalClock tick or any RowSet changes.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralPrevMappings = null;

    protected GroupingRowSetHelper(final OrderedLongSet impl) {
        super(impl);
    }

    @Override
    public void close() {
        clearMappings();
        super.close();
    }

    @Override
    protected void postMutationHook() {
        clearMappings();
    }

    private static class MappingInfo {

        private final TupleSource tupleSource;
        private final Map<Object, RowSet> mapping;
        private final long creationTick;

        private MappingInfo(final TupleSource tupleSource, final Map<Object, RowSet> mapping, final long creationTick) {
            this.tupleSource = tupleSource;
            this.mapping = mapping;
            this.creationTick = creationTick;
        }
    }

    private Map<Object, RowSet> lookupMapping(List<ColumnSource> columnSourceKey) {
        return lookupMapping(mappings, ephemeralMappings, columnSourceKey);
    }

    private static Map<Object, RowSet> lookupMapping(
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            List<ColumnSource> columnSourceKey) {
        final Map<Object, RowSet> immutableMapping = lookupImmutableMapping(mappings, columnSourceKey);
        if (immutableMapping != null) {
            return immutableMapping;
        }
        return lookupEphemeralMapping(columnSourceKey, ephemeralMappings);
    }

    private Map<Object, RowSet> lookupPrevMapping(List<ColumnSource> columnSourceKey) {
        return lookupPrevMapping(mappings, ephemeralPrevMappings, columnSourceKey);
    }

    private static Map<Object, RowSet> lookupPrevMapping(
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralPrevMappings,
            List<ColumnSource> columnSourceKey) {
        final Map<Object, RowSet> immutableMapping = lookupImmutableMapping(mappings, columnSourceKey);
        if (immutableMapping != null) {
            return immutableMapping;
        }
        return lookupEphemeralMapping(columnSourceKey, ephemeralPrevMappings);
    }

    private static Map<Object, RowSet> lookupImmutableMapping(
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            List<ColumnSource> columnSourceKey) {
        if (mappings == null) {
            return null;
        }
        final MappingInfo mappingInfo = mappings.get(columnSourceKey);
        if (mappingInfo == null) {
            return null;
        }
        return mappingInfo.mapping;
    }

    private static Map<Object, RowSet> lookupEphemeralMapping(List<ColumnSource> columnSourceKey,
            WeakHashMap<List<ColumnSource>, MappingInfo> groupingMap) {
        if (groupingMap == null) {
            return null;
        }

        final MappingInfo resultInfo = groupingMap.get(columnSourceKey);
        if (resultInfo == null) {
            return null;
        }

        if (resultInfo.creationTick != LogicalClock.DEFAULT.currentStep()) {
            groupingMap.remove(columnSourceKey);
            return null;
        }

        return resultInfo.mapping;
    }

    @Override
    public boolean hasGrouping(final ColumnSource... keyColumns) {
        if (keyColumns.length == 0) {
            return true;
        }
        final List<ColumnSource> sourcesKey = Arrays.asList(keyColumns);
        final Map<Object, RowSet> groupingCandidate = lookupMapping(sourcesKey);
        return groupingCandidate != null || keyColumns.length == 1 && keyColumns[0].getGroupToRange() != null;
    }

    public static Map<Object, RowSet> getGrouping(
            final RowSet thisRowSet,
            final UnaryOperator<RowSet> indexOp,
            final WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            final WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            final TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();
        final Map<Object, RowSet> lookupResult = lookupMapping(mappings, ephemeralMappings, sourcesKey);
        if (lookupResult != null) {
            return lookupResult;
        }

        final Map<Object, RowSet> result = new LinkedHashMap<>();
        final BiConsumer<Object, RowSet> resultCollector = result::put;
        collectGrouping(thisRowSet, indexOp, mappings, ephemeralMappings, resultCollector, tupleSource, sourcesKey);
        return result;
    }

    @Override
    public Map<Object, RowSet> getGrouping(final TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();
        final Map<Object, RowSet> lookupResult = lookupMapping(sourcesKey);
        if (lookupResult != null) {
            return lookupResult;
        }

        final Map<Object, RowSet> result = new LinkedHashMap<>();
        final BiConsumer<Object, RowSet> resultCollector = result::put;
        collectGrouping(this, this::intersect, mappings, ephemeralMappings, resultCollector, tupleSource, sourcesKey);

        if (areColumnsImmutable(sourcesKey)) {
            if (mappings == null) {
                mappings = new WeakHashMap<>();
            }
            mappings.put(sourcesKey, new MappingInfo(tupleSource, result, 0));
        } else {
            if (ephemeralMappings == null) {
                ephemeralMappings = new WeakHashMap<>();
            }
            ephemeralMappings.put(sourcesKey, new MappingInfo(tupleSource, result, LogicalClock.DEFAULT.currentStep()));
        }

        return result;
    }

    @Override
    public void copyImmutableGroupings(final TupleSource source, final TupleSource dest) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = source.getColumnSources();
        // noinspection unchecked
        final List<ColumnSource> destKey = dest.getColumnSources();

        // copy immutable groupings
        if (mappings != null) {
            final MappingInfo toCopy;
            if ((toCopy = mappings.get(sourcesKey)) != null) {
                mappings.put(destKey, toCopy);
            }
        }
    }

    private static void collectGrouping(
            final RowSet thisRowSet,
            final UnaryOperator<RowSet> indexOp,
            final WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            final WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            final BiConsumer<Object, RowSet> resultCollector,
            final TupleSource tupleSource,
            final List<ColumnSource> keyColumns) {
        if (keyColumns.isEmpty()) {
            resultCollector.accept(EmptyTuple.INSTANCE, thisRowSet.copy());
        } else if (keyColumns.size() == 1 && keyColumns.get(0).getGroupToRange() != null) {
            @SuppressWarnings("unchecked")
            final Map<Object, RowSet> sourceGrouping = keyColumns.get(0).getGroupToRange();
            for (final Map.Entry<Object, RowSet> objectIndexEntry : sourceGrouping.entrySet()) {
                final RowSet resultRowSet = indexOp.apply(objectIndexEntry.getValue());
                if (resultRowSet.size() > 0) {
                    resultCollector.accept(objectIndexEntry.getKey(), resultRowSet);
                }
            }
        } else {
            final long columnsWithGrouping = keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).count();
            final boolean canUseAllConstituents = columnsWithGrouping == keyColumns.size();
            final boolean canUseAnyConstituents = columnsWithGrouping > 0;

            if (canUseAllConstituents) {
                // we can generate a grouping using just the pre-existing groupings
                generateGrouping(indexOp, resultCollector, tupleSource, keyColumns, 0, new Object[keyColumns.size()],
                        null);
            } else if (canUseAnyConstituents) {
                generatePartialGrouping(thisRowSet, indexOp, mappings, ephemeralMappings, resultCollector, tupleSource,
                        keyColumns);
            } else {
                final Map<Object, RowSetBuilderSequential> resultBuilder = new LinkedHashMap<>();
                for (final RowSet.Iterator iterator = thisRowSet.iterator(); iterator.hasNext();) {
                    final long next = iterator.nextLong();
                    final Object key = tupleSource.createTuple(next);
                    resultBuilder.computeIfAbsent(key, k -> RowSetFactory.builderSequential())
                            .appendKey(next);
                }
                resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.build()));
            }
        }
    }

    private static void generatePartialGrouping(
            final RowSet thisRowSet,
            final UnaryOperator<RowSet> indexOp,
            final WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            final WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            final BiConsumer<Object, RowSet> resultCollector, TupleSource tupleSource,
            final List<ColumnSource> keyColumns) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        final TupleSource groupedTupleSource = TupleSourceFactory.makeTupleSource(groupedKeyColumns);
        final Map<Object, RowSet> groupedColumnsGrouping =
                getGrouping(thisRowSet, indexOp, mappings, ephemeralMappings, groupedTupleSource);
        generatePartialGroupingSecondHalf(groupedKeyColumns, notGroupedKeyColumns, groupedTupleSource,
                groupedColumnsGrouping,
                resultCollector, tupleSource, keyColumns);
    }

    private void generatePartialGrouping(final BiConsumer<Object, RowSet> resultCollector,
            final TupleSource tupleSource,
            final List<ColumnSource> keyColumns) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        final TupleSource groupedTupleSource = TupleSourceFactory.makeTupleSource(groupedKeyColumns);
        final Map<Object, RowSet> groupedColumnsGrouping = getGrouping(groupedTupleSource);
        generatePartialGroupingSecondHalf(groupedKeyColumns, notGroupedKeyColumns, groupedTupleSource,
                groupedColumnsGrouping,
                resultCollector, tupleSource, keyColumns);
    }

    private static void generatePartialGroupingSecondHalf(
            final ColumnSource[] groupedKeyColumns, final ColumnSource[] notGroupedKeyColumns,
            final TupleSource groupedTupleSource, final Map<Object, RowSet> groupedColumnsGrouping,
            final BiConsumer<Object, RowSet> resultCollector, final TupleSource tupleSource,
            final List<ColumnSource> keyColumns) {
        final Map<Object, RowSetBuilderSequential> resultBuilder = new LinkedHashMap<>();

        final int[] groupedKeysIndices = new int[groupedKeyColumns.length];
        final int[] notGroupedKeysIndices = new int[notGroupedKeyColumns.length];
        int jj = 0, kk = 0;
        for (int ii = 0; ii < keyColumns.size(); ++ii) {
            if (keyColumns.get(ii).getGroupToRange() != null) {
                groupedKeysIndices[jj++] = ii;
            } else {
                notGroupedKeysIndices[kk++] = ii;
            }
        }

        for (final Map.Entry<Object, RowSet> entry : groupedColumnsGrouping.entrySet()) {
            final Object[] partialKeyValues = new Object[keyColumns.size()];
            if (groupedKeyColumns.length == 1) {
                partialKeyValues[groupedKeysIndices[0]] = entry.getKey();
            } else {
                final Object groupedTuple = entry.getKey();
                for (int ii = 0; ii < groupedKeysIndices.length; ++ii) {
                    // noinspection unchecked
                    partialKeyValues[groupedKeysIndices[ii]] =
                            groupedTupleSource.exportElementReinterpreted(groupedTuple, ii);
                }
            }

            final RowSet resultRowSet = entry.getValue();
            for (final RowSet.Iterator iterator = resultRowSet.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();

                for (int ii = 0; ii < notGroupedKeysIndices.length; ++ii) {
                    partialKeyValues[notGroupedKeysIndices[ii]] = notGroupedKeyColumns[ii].get(next);
                }

                resultBuilder.computeIfAbsent(tupleSource.createTupleFromReinterpretedValues(partialKeyValues),
                        k -> RowSetFactory.builderSequential()).appendKey(next);
            }
        }

        resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.build()));
    }

    private void generatePartialGroupingForKeySet(final BiConsumer<Object, RowSet> resultCollector,
            final TupleSource tupleSource,
            final List<ColumnSource> keyColumns, Set<Object> keys) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        Require.gtZero(groupedKeyColumns.length, "groupedKeyColumns.length");
        Require.gtZero(notGroupedKeyColumns.length, "notGroupedKeyColumns.length");

        final TupleSource groupedTupleSource = TupleSourceFactory.makeTupleSource(groupedKeyColumns);

        final Map<Object, RowSetBuilderSequential> resultBuilder = new LinkedHashMap<>();

        final int[] groupedKeysIndices = new int[groupedKeyColumns.length];
        final int[] notGroupedKeysIndices = new int[notGroupedKeyColumns.length];
        int jj = 0, kk = 0;
        for (int ii = 0; ii < keyColumns.size(); ++ii) {
            if (keyColumns.get(ii).getGroupToRange() != null) {
                groupedKeysIndices[jj++] = ii;
            } else {
                notGroupedKeysIndices[kk++] = ii;
            }
        }

        final Set<Object> groupPruningSet = new HashSet<>();
        if (groupedKeysIndices.length == 1) {
            // noinspection unchecked
            keys.forEach(x -> groupPruningSet.add(tupleSource.exportElementReinterpreted(x, groupedKeysIndices[0])));
        } else {
            final Object[] groupingKeyValues = new Object[groupedKeysIndices.length];
            keys.forEach(x -> {
                for (int ii = 0; ii < groupingKeyValues.length; ++ii) {
                    // noinspection unchecked
                    groupingKeyValues[ii] = tupleSource.exportElementReinterpreted(x, groupedKeysIndices[ii]);
                }
                groupPruningSet.add(groupedTupleSource.createTupleFromReinterpretedValues(groupingKeyValues));
            });
        }

        final Map<Object, RowSet> groupedColumnsGrouping =
                getGroupingForKeySet(groupPruningSet, groupedTupleSource);

        final Object[] lookupKeyValues = new Object[keyColumns.size()];

        for (final Map.Entry<Object, RowSet> entry : groupedColumnsGrouping.entrySet()) {
            final RowSet resultRowSet = entry.getValue().intersect(this);
            if (resultRowSet.isEmpty()) {
                continue;
            }

            if (groupedKeyColumns.length == 1) {
                lookupKeyValues[groupedKeysIndices[0]] = entry.getKey();
            } else {
                for (int ii = 0; ii < groupedKeysIndices.length; ++ii) {
                    // noinspection unchecked
                    lookupKeyValues[groupedKeysIndices[ii]] =
                            groupedTupleSource.exportElementReinterpreted(entry.getKey(), ii);
                }
            }

            for (final RowSet.Iterator iterator = resultRowSet.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();

                for (int ii = 0; ii < notGroupedKeysIndices.length; ++ii) {
                    lookupKeyValues[notGroupedKeysIndices[ii]] = notGroupedKeyColumns[ii].get(next);
                }

                final Object key = tupleSource.createTupleFromReinterpretedValues(lookupKeyValues);
                if (!keys.contains(key)) {
                    continue;
                }

                final RowSetBuilderSequential indexForKey =
                        resultBuilder.computeIfAbsent(key, k -> RowSetFactory.builderSequential());
                indexForKey.appendKey(next);
            }
        }

        resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.build()));
    }

    @Override
    public Map<Object, RowSet> getGroupingForKeySet(final Set<Object> keys, final TupleSource tupleSource) {
        final Map<Object, RowSet> result = new LinkedHashMap<>();
        collectGroupingForKeySet(keys, tupleSource, result::put);
        return result;
    }

    @Override
    public RowSet getSubSetForKeySet(final Set<Object> keys, final TupleSource tupleSource) {
        final RowSetBuilderRandom rowSetBuilder = RowSetFactory.builderRandom();
        final BiConsumer<Object, RowSet> resultCollector =
                (key, index) -> rowSetBuilder.addRowSet(index);
        collectGroupingForKeySet(keys, tupleSource, resultCollector);
        return rowSetBuilder.build();
    }

    private void collectGroupingForKeySet(final Set<Object> keys, final TupleSource tupleSource,
            final BiConsumer<Object, RowSet> resultCollector) {
        // noinspection unchecked
        final List<ColumnSource> keyColumns = tupleSource.getColumnSources();
        if (keyColumns.isEmpty()) {
            resultCollector.accept(EmptyTuple.INSTANCE, this.copy());
        } else if (keyColumns.size() == 1 && keyColumns.get(0).getGroupToRange() != null) {
            @SuppressWarnings("unchecked")
            final Map<Object, RowSet> sourceGrouping = keyColumns.get(0).getGroupToRange();
            sourceGrouping.entrySet().stream().filter(objectIndexEntry -> keys.contains(objectIndexEntry.getKey()))
                    .forEach(objectIndexEntry -> {
                        final RowSet resultRowSet = objectIndexEntry.getValue().intersect(this);
                        if (resultRowSet.size() > 0) {
                            resultCollector.accept(objectIndexEntry.getKey(), resultRowSet);
                        }
                    });
        } else {
            final long columnsWithGrouping = keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).count();
            final boolean canUseAllConstituents = columnsWithGrouping == keyColumns.size();
            final boolean canUseAnyConstituents = columnsWithGrouping > 0;

            if (canUseAllConstituents) {
                generateGrouping(resultCollector, tupleSource, keyColumns, 0, new Object[keyColumns.size()], null,
                        keys);
            } else if (canUseAnyConstituents) {
                generatePartialGroupingForKeySet(resultCollector, tupleSource, keyColumns, keys);
            } else {
                final Map<Object, RowSetBuilderSequential> resultBuilder = new LinkedHashMap<>();
                for (final RowSet.Iterator iterator = this.iterator(); iterator.hasNext();) {
                    final long next = iterator.nextLong();
                    final Object key = tupleSource.createTuple(next);
                    if (keys.contains(key)) {
                        resultBuilder.computeIfAbsent(key, k -> RowSetFactory.builderSequential())
                                .appendKey(next);
                    }
                }
                for (final Map.Entry<Object, RowSetBuilderSequential> objectIndexBuilderEntry : resultBuilder
                        .entrySet()) {
                    resultCollector.accept(objectIndexBuilderEntry.getKey(),
                            objectIndexBuilderEntry.getValue().build());
                }
            }
        }
    }

    private static void generateGrouping(
            final UnaryOperator<RowSet> indexOp,
            final BiConsumer<Object, RowSet> resultCollector,
            final TupleSource tupleSource,
            final List<ColumnSource> keyColumns,
            final int position,
            final Object[] partialValues,
            final RowSet partiallyIntersectedRowSet) {
        for (final Object objectEntry : keyColumns.get(position).getGroupToRange().entrySet()) {
            // noinspection unchecked
            final Map.Entry<Object, RowSet> entry =
                    (Map.Entry<Object, RowSet>) objectEntry;
            partialValues[position] = entry.getKey();
            final RowSet subRowSet;
            if (position == 0) {
                subRowSet = indexOp.apply(entry.getValue());
            } else {
                subRowSet = partiallyIntersectedRowSet.intersect(entry.getValue());
            }
            if (subRowSet.isNonempty()) {
                if (position == keyColumns.size() - 1) {
                    // we're at the very last bit, so we should start shoving our tuples into the result map
                    resultCollector.accept(tupleSource.createTupleFromReinterpretedValues(partialValues), subRowSet);
                } else {
                    generateGrouping(indexOp, resultCollector, tupleSource, keyColumns, position + 1, partialValues,
                            subRowSet);
                }
            }
        }
    }

    private void generateGrouping(final BiConsumer<Object, RowSet> resultCollector, final TupleSource tupleSource,
            final List<ColumnSource> keyColumns, final int position, final Object[] partialValues,
            final RowSet partiallyIntersectedRowSet,
            final Set<Object> keyRestriction) {
        final boolean finalPosition = position == keyColumns.size() - 1;

        final List<ColumnSource> subSources = keyColumns.subList(0, position + 1);
        final TupleSource subTupleSource =
                TupleSourceFactory.makeTupleSource(subSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY));

        final Set<Object> pruningSet;
        if (finalPosition) {
            pruningSet = keyRestriction;
        } else if (position == 0) {
            // noinspection unchecked
            pruningSet = keyRestriction.stream().map(x -> tupleSource.exportElementReinterpreted(x, 0))
                    .collect(Collectors.toCollection(HashSet::new));
        } else {
            pruningSet = new HashSet<>();
            final Object[] partialKey = new Object[position + 1];
            keyRestriction.forEach(key -> {
                for (int ii = 0; ii < partialKey.length; ++ii) {
                    // noinspection unchecked
                    partialKey[ii] = tupleSource.exportElementReinterpreted(key, ii);
                }
                pruningSet.add(subTupleSource.createTupleFromReinterpretedValues(partialKey));
            });
        }

        // noinspection unchecked
        final Map<Object, RowSet> groupToRange =
                (Map<Object, RowSet>) keyColumns.get(position).getGroupToRange();
        final Object[] pruningKey = Arrays.copyOf(partialValues, position + 1);
        for (final Map.Entry<Object, RowSet> entry : groupToRange.entrySet()) {
            pruningKey[position] = partialValues[position] = entry.getKey();

            final Object tuple;
            if (finalPosition) {
                tuple = tupleSource.createTupleFromReinterpretedValues(partialValues);
                if (!keyRestriction.contains(tuple)) {
                    continue;
                }
            } else if (position == 0) {
                if (!pruningSet.contains(entry.getKey())) {
                    continue;
                }

                tuple = null;
            } else {
                if (!pruningSet.contains(subTupleSource.createTupleFromReinterpretedValues(pruningKey))) {
                    continue;
                }

                tuple = null;
            }

            final RowSet subRowSet;
            if (position == 0) {
                subRowSet = intersect(entry.getValue());
            } else {
                subRowSet = partiallyIntersectedRowSet.intersect(entry.getValue());
            }

            if (subRowSet.isNonempty()) {
                if (finalPosition) {
                    // we're at the very last bit, so we should start shoving our smart keys into the result map
                    resultCollector.accept(tuple, subRowSet);
                } else {
                    generateGrouping(resultCollector, tupleSource, keyColumns, position + 1, partialValues, subRowSet,
                            keyRestriction);
                }
            }
        }
    }

    @Override
    public Map<Object, RowSet> getPrevGrouping(final TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();

        Map<Object, RowSet> result = lookupPrevMapping(sourcesKey);
        if (result != null) {
            return result;
        }
        result = new LinkedHashMap<>();
        if (sourcesKey.isEmpty()) {
            result.put(EmptyTuple.INSTANCE, this.copy());
        } else {
            final Map<Object, RowSetBuilderSequential> resultBuilder = new LinkedHashMap<>();
            for (final RowSet.Iterator iterator = this.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();
                final Object key = tupleSource.createPreviousTuple(next);
                resultBuilder.computeIfAbsent(key, k -> RowSetFactory.builderSequential())
                        .appendKey(next);
            }
            result = new LinkedHashMap<>();
            for (final Map.Entry<Object, RowSetBuilderSequential> objectIndexBuilderEntry : resultBuilder.entrySet()) {
                result.put(objectIndexBuilderEntry.getKey(), objectIndexBuilderEntry.getValue().build());
            }
        }
        if (areColumnsImmutable(sourcesKey)) {
            if (mappings == null) {
                mappings = new WeakHashMap<>();
            }
            mappings.put(sourcesKey, new MappingInfo(tupleSource, result, 0));
        } else {
            if (ephemeralPrevMappings == null) {
                ephemeralPrevMappings = new WeakHashMap<>();
            }
            ephemeralPrevMappings.put(sourcesKey,
                    new MappingInfo(tupleSource, result, LogicalClock.DEFAULT.currentStep()));
        }
        return result;
    }

    private boolean areColumnsImmutable(final List<ColumnSource> sourcesKey) {
        return sourcesKey.stream().allMatch(ColumnSource::isImmutable);
    }

    public void clearMappings() {
        if (mappings != null) {
            mappings.clear();
        }
        clearEphemeralMappings();
    }

    private void clearEphemeralMappings() {
        if (ephemeralMappings != null) {
            ephemeralMappings.clear();
        }
        if (ephemeralPrevMappings != null) {
            ephemeralPrevMappings.clear();
        }
    }
}
