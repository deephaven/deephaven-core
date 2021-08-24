/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.util.tuples.EmptyTuple;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LogicalClock;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import gnu.trove.list.array.TLongArrayList;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

public abstract class SortedIndex extends OrderedKeysAsChunkImpl implements Index {

    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override
    public Index clone() {
        throw new UnsupportedOperationException();
    }

    /**
     * These mappings last forever, and only include column sources that are immutable.
     *
     * Whenever this index is changed, we must clear the mappings.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> mappings = null;

    private static class MappingInfo {
        private final TupleSource tupleSource;
        private final Map<Object, Index> mapping;
        private final long creationTick;

        private MappingInfo(Map<Object, Index> mapping, long creationTick,
            TupleSource tupleSource) {
            this.mapping = mapping;
            this.creationTick = creationTick;
            this.tupleSource = tupleSource;
        }
    }

    /**
     * These mappings do not survive past a single LogicalClock tick or any index changes.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings = null;
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralPrevMappings = null;

    protected final void onUpdate(final ReadOnlyIndex added, final ReadOnlyIndex removed) {
        clearMappings();
    }

    protected final void onRemove(final ReadOnlyIndex removed) {
        clearMappings();
    }

    protected final void onRetain(final ReadOnlyIndex intersected) {
        clearMappings();
    }

    protected final void onInsert(final ReadOnlyIndex added) {
        clearMappings();
    }

    protected final void onClear() {
        clearMappings();
    }

    @Override
    public final boolean isSorted() {
        return true;
    }

    /**
     * @return Whether this index is flat (that is, contiguous from 0 to size - 1)
     */
    public final boolean isFlat() {
        return empty() || (lastKey() == size() - 1);
    }

    @Override
    public void insert(final ReadOnlyIndex added) {
        for (final Index.RangeIterator iterator = added.rangeIterator(); iterator.hasNext();) {
            iterator.next();
            insertRange(iterator.currentRangeStart(), iterator.currentRangeEnd());
        }
    }

    @Override
    public void remove(ReadOnlyIndex removed) {
        for (final Index.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            remove(next);
        }
    }

    @Override
    public Index invert(ReadOnlyIndex keys) {
        return invert(keys, Long.MAX_VALUE);
    }

    /**
     * The only used implementation of invert is in the TreeIndex, really the guts of it are in
     * BspNodeIndex.
     *
     * This version is inefficient as it simply performs O(keys) find operations; which is O(keys *
     * lg size), because there is no memory about what you've already found.
     *
     * It serves as a reasonable reference for what the invert operation is "meant" to do.
     *
     * Note maximumPosition is inclusive.
     */
    @Override
    public Index invert(final ReadOnlyIndex keys, final long maximumPosition) {
        final Index.SequentialBuilder indexBuilder = Index.FACTORY.getSequentialBuilder();
        for (final Index.Iterator iterator = keys.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            final long position = find(next);
            if (position > maximumPosition) {
                break;
            }
            Assert.geqZero(position, "position");
            indexBuilder.appendKey(position);
        }
        return indexBuilder.getIndex();
    }

    @Override
    public TLongArrayList[] findMissing(ReadOnlyIndex keys) {
        return IndexUtilities.findMissing(this, keys);
    }

    private Map<Object, Index> lookupMapping(List<ColumnSource> columnSourceKey) {
        return lookupMapping(mappings, ephemeralMappings, columnSourceKey);
    }

    private static Map<Object, Index> lookupMapping(
        WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
        WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
        List<ColumnSource> columnSourceKey) {
        final Map<Object, Index> immutableMapping =
            lookupImmutableMapping(mappings, columnSourceKey);
        if (immutableMapping != null)
            return immutableMapping;
        return lookupEphemeralMapping(columnSourceKey, ephemeralMappings);
    }

    private static Map<Object, Index> lookupPrevMapping(
        WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
        WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralPrevMappings,
        List<ColumnSource> columnSourceKey) {
        final Map<Object, Index> immutableMapping =
            lookupImmutableMapping(mappings, columnSourceKey);
        if (immutableMapping != null)
            return immutableMapping;
        return lookupEphemeralMapping(columnSourceKey, ephemeralPrevMappings);
    }

    private Map<Object, Index> lookupPrevMapping(List<ColumnSource> columnSourceKey) {
        return lookupPrevMapping(mappings, ephemeralPrevMappings, columnSourceKey);
    }

    private static Map<Object, Index> lookupImmutableMapping(
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

    private static Map<Object, Index> lookupEphemeralMapping(List<ColumnSource> columnSourceKey,
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
    public String toString() {
        String result = "{";
        boolean isFirst = true;
        for (Index.Iterator it = this.iterator(); it.hasNext();) {
            long next = it.nextLong();
            result += (isFirst ? "" : ",") + next;
            isFirst = false;
        }
        result += "}";
        return result;
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object obj) {
        return IndexUtilities.equals(this, obj);
    }

    /**
     * On an insertion operation, we clear all of our mappings so that they are not out-of-date.
     */
    protected void updateGroupingOnInsert(long key) {
        clearMappings();
    }

    /**
     * On an insertion operation, we clear all of our mappings so that they are not out-of-date.
     */
    protected void updateGroupingOnInsert(final LongChunk<OrderedKeyIndices> keys, final int offset,
        final int length) {
        clearMappings();
    }

    /**
     * On an insertion operation, we clear all of our mappings so that they are not out-of-date.
     */
    protected void updateGroupOnInsertRange(final long start, final long end) {
        clearMappings();
    }

    /**
     * On an insertion operation, we clear all of our mappings so that they are not out-of-date.
     */
    protected void updateGroupingOnRemove(final long key) {
        clearMappings();
    }

    /**
     * On an insertion operation, we clear all of our mappings so that they are not out-of-date.
     */
    protected void updateGroupingOnRemoveRange(final long start, final long end) {
        clearMappings();
    }

    /**
     * On an insertion operation, we clear all of our mappings so that they are not out-of-date.
     */
    protected void updateGroupingOnRemove(final LongChunk<OrderedKeyIndices> keys, final int offset,
        final int length) {
        clearMappings();
    }

    protected void updateGroupingOnRetainRange(final long start, final long end) {
        clearMappings();
    }

    protected void updateGroupingOnRetain(final Index intersected) {
        clearMappings();
    }

    public static Map<Object, Index> getGrouping(
        final Index thisIndex,
        UnaryOperator<Index> indexOp,
        WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
        WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
        TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();
        final Map<Object, Index> lookupResult =
            lookupMapping(mappings, ephemeralMappings, sourcesKey);
        if (lookupResult != null) {
            return lookupResult;
        }

        final Map<Object, Index> result = new LinkedHashMap<>();

        BiConsumer<Object, Index> resultCollector = result::put;

        collectGrouping(thisIndex, indexOp, mappings, ephemeralMappings, resultCollector,
            tupleSource, sourcesKey);

        return result;
    }

    @Override
    public Map<Object, Index> getGrouping(TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();
        final Map<Object, Index> lookupResult = lookupMapping(sourcesKey);
        if (lookupResult != null) {
            return lookupResult;
        }

        final Map<Object, Index> result = new LinkedHashMap<>();

        final BiConsumer<Object, Index> resultCollector = result::put;

        collectGrouping(this, this::intersect, mappings, ephemeralMappings, resultCollector,
            tupleSource, sourcesKey);

        // TODO: We need to do something better than weakly-reachable List<ColumnSource>s here. Keys
        // are probably cleaned up well before we want right now. Values only cleaned up on access.
        // Both are sub-par.
        if (areColumnsImmutable(sourcesKey)) {
            if (mappings == null) {
                mappings = new WeakHashMap<>();
            }
            mappings.put(sourcesKey, new MappingInfo(result, 0, tupleSource));
        } else {
            if (ephemeralMappings == null) {
                ephemeralMappings = new WeakHashMap<>();
            }
            ephemeralMappings.put(sourcesKey,
                new MappingInfo(result, LogicalClock.DEFAULT.currentStep(), tupleSource));
        }

        return result;
    }

    @Override
    public void copyImmutableGroupings(TupleSource source, TupleSource dest) {
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
        final Index thisIndex,
        final UnaryOperator<Index> indexOp,
        WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
        WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
        final BiConsumer<Object, Index> resultCollector,
        final TupleSource tupleSource,
        final List<ColumnSource> keyColumns) {
        if (keyColumns.isEmpty()) {
            resultCollector.accept(EmptyTuple.INSTANCE, thisIndex.clone());
        } else if (keyColumns.size() == 1 && keyColumns.get(0).getGroupToRange() != null) {
            @SuppressWarnings("unchecked")
            final Map<Object, Index> sourceGrouping = keyColumns.get(0).getGroupToRange();
            for (Map.Entry<Object, Index> objectIndexEntry : sourceGrouping.entrySet()) {
                final Index resultIndex = indexOp.apply(objectIndexEntry.getValue());
                if (resultIndex.size() > 0) {
                    resultCollector.accept(objectIndexEntry.getKey(), resultIndex);
                }
            }
        } else {
            final long columnsWithGrouping =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).count();
            final boolean canUseAllConstituents = columnsWithGrouping == keyColumns.size();
            final boolean canUseAnyConstituents = columnsWithGrouping > 0;

            if (canUseAllConstituents) {
                // we can generate a grouping using just the pre-existing groupings
                generateGrouping(indexOp, resultCollector, tupleSource, keyColumns, 0,
                    new Object[keyColumns.size()], null);
            } else if (canUseAnyConstituents) {
                generatePartialGrouping(thisIndex, indexOp, mappings, ephemeralMappings,
                    resultCollector, tupleSource, keyColumns);
            } else {
                final Map<Object, SequentialBuilder> resultBuilder = new LinkedHashMap<>();
                for (final Index.Iterator iterator = thisIndex.iterator(); iterator.hasNext();) {
                    final long next = iterator.nextLong();
                    final Object key = tupleSource.createTuple(next);
                    resultBuilder.computeIfAbsent(key, k -> Index.FACTORY.getSequentialBuilder())
                        .appendKey(next);
                }
                resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.getIndex()));
            }
        }
    }

    private static void generatePartialGrouping(
        final Index thisIndex,
        final UnaryOperator<Index> indexOp,
        final WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
        final WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
        final BiConsumer<Object, Index> resultCollector, TupleSource tupleSource,
        List<ColumnSource> keyColumns) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns = keyColumns.stream()
            .filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns = keyColumns.stream()
            .filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        final TupleSource groupedTupleSource =
            TupleSourceFactory.makeTupleSource(groupedKeyColumns);
        final Map<Object, Index> groupedColumnsGrouping =
            getGrouping(thisIndex, indexOp, mappings, ephemeralMappings, groupedTupleSource);
        generatePartialGroupingSecondHalf(groupedKeyColumns, notGroupedKeyColumns,
            groupedTupleSource, groupedColumnsGrouping,
            resultCollector, tupleSource, keyColumns);
    }

    private void generatePartialGrouping(BiConsumer<Object, Index> resultCollector,
        TupleSource tupleSource, List<ColumnSource> keyColumns) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns = keyColumns.stream()
            .filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns = keyColumns.stream()
            .filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        final TupleSource groupedTupleSource =
            TupleSourceFactory.makeTupleSource(groupedKeyColumns);
        final Map<Object, Index> groupedColumnsGrouping = getGrouping(groupedTupleSource);
        generatePartialGroupingSecondHalf(groupedKeyColumns, notGroupedKeyColumns,
            groupedTupleSource, groupedColumnsGrouping,
            resultCollector, tupleSource, keyColumns);
    }

    private static void generatePartialGroupingSecondHalf(
        final ColumnSource[] groupedKeyColumns, final ColumnSource[] notGroupedKeyColumns,
        final TupleSource groupedTupleSource, final Map<Object, Index> groupedColumnsGrouping,
        final BiConsumer<Object, Index> resultCollector, final TupleSource tupleSource,
        final List<ColumnSource> keyColumns) {
        final Map<Object, SequentialBuilder> resultBuilder = new LinkedHashMap<>();

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

        for (Map.Entry<Object, Index> entry : groupedColumnsGrouping.entrySet()) {
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

            final Index resultIndex = entry.getValue();
            for (final Index.Iterator iterator = resultIndex.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();

                for (int ii = 0; ii < notGroupedKeysIndices.length; ++ii) {
                    partialKeyValues[notGroupedKeysIndices[ii]] =
                        notGroupedKeyColumns[ii].get(next);
                }

                resultBuilder.computeIfAbsent(
                    tupleSource.createTupleFromReinterpretedValues(partialKeyValues),
                    k -> Index.FACTORY.getSequentialBuilder()).appendKey(next);
            }
        }

        resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.getIndex()));
    }

    private void generatePartialGroupingForKeySet(BiConsumer<Object, Index> resultCollector,
        TupleSource tupleSource, List<ColumnSource> keyColumns, Set<Object> keys) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns = keyColumns.stream()
            .filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns = keyColumns.stream()
            .filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        Require.gtZero(groupedKeyColumns.length, "groupedKeyColumns.length");
        Require.gtZero(notGroupedKeyColumns.length, "notGroupedKeyColumns.length");

        final TupleSource groupedTupleSource =
            TupleSourceFactory.makeTupleSource(groupedKeyColumns);

        final Map<Object, SequentialBuilder> resultBuilder = new LinkedHashMap<>();

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
            keys.forEach(x -> groupPruningSet
                .add(tupleSource.exportElementReinterpreted(x, groupedKeysIndices[0])));
        } else {
            final Object[] groupingKeyValues = new Object[groupedKeysIndices.length];
            keys.forEach(x -> {
                for (int ii = 0; ii < groupingKeyValues.length; ++ii) {
                    // noinspection unchecked
                    groupingKeyValues[ii] =
                        tupleSource.exportElementReinterpreted(x, groupedKeysIndices[ii]);
                }
                groupPruningSet
                    .add(groupedTupleSource.createTupleFromReinterpretedValues(groupingKeyValues));
            });
        }

        final Map<Object, Index> groupedColumnsGrouping =
            getGroupingForKeySet(groupPruningSet, groupedTupleSource);

        final Object[] lookupKeyValues = new Object[keyColumns.size()];

        for (Map.Entry<Object, Index> entry : groupedColumnsGrouping.entrySet()) {
            final Index resultIndex = entry.getValue().intersect(this);
            if (resultIndex.empty()) {
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

            for (final Index.Iterator iterator = resultIndex.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();

                for (int ii = 0; ii < notGroupedKeysIndices.length; ++ii) {
                    lookupKeyValues[notGroupedKeysIndices[ii]] = notGroupedKeyColumns[ii].get(next);
                }

                final Object key = tupleSource.createTupleFromReinterpretedValues(lookupKeyValues);
                if (!keys.contains(key)) {
                    continue;
                }

                final SequentialBuilder indexForKey =
                    resultBuilder.computeIfAbsent(key, k -> Index.FACTORY.getSequentialBuilder());
                indexForKey.appendKey(next);
            }
        }

        resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.getIndex()));
    }

    @Override
    public Map<Object, Index> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource) {
        final Map<Object, Index> result = new LinkedHashMap<>();

        collectGroupingForKeySet(keys, tupleSource, result::put);

        return result;
    }

    @Override
    public Index getSubIndexForKeySet(Set<Object> keys, TupleSource tupleSource) {
        final IndexBuilder indexBuilder = Index.FACTORY.getBuilder();
        final BiConsumer<Object, Index> resultCollector =
            (key, index) -> indexBuilder.addIndex(index);

        collectGroupingForKeySet(keys, tupleSource, resultCollector);

        return indexBuilder.getIndex();
    }

    private void collectGroupingForKeySet(Set<Object> keys, TupleSource tupleSource,
        BiConsumer<Object, Index> resultCollector) {
        // noinspection unchecked
        final List<ColumnSource> keyColumns = tupleSource.getColumnSources();
        if (keyColumns.isEmpty()) {
            resultCollector.accept(EmptyTuple.INSTANCE, this.clone());
        } else if (keyColumns.size() == 1 && keyColumns.get(0).getGroupToRange() != null) {
            @SuppressWarnings("unchecked")
            final Map<Object, Index> sourceGrouping = keyColumns.get(0).getGroupToRange();
            sourceGrouping.entrySet().stream()
                .filter(objectIndexEntry -> keys.contains(objectIndexEntry.getKey()))
                .forEach(objectIndexEntry -> {
                    final Index resultIndex = objectIndexEntry.getValue().intersect(this);
                    if (resultIndex.size() > 0) {
                        resultCollector.accept(objectIndexEntry.getKey(), resultIndex);
                    }
                });
        } else {
            final long columnsWithGrouping =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).count();
            final boolean canUseAllConstituents = columnsWithGrouping == keyColumns.size();
            final boolean canUseAnyConstituents = columnsWithGrouping > 0;

            if (canUseAllConstituents) {
                generateGrouping(resultCollector, tupleSource, keyColumns, 0,
                    new Object[keyColumns.size()], null, keys);
            } else if (canUseAnyConstituents) {
                generatePartialGroupingForKeySet(resultCollector, tupleSource, keyColumns, keys);
            } else {
                final Map<Object, SequentialBuilder> resultBuilder = new LinkedHashMap<>();
                for (final Index.Iterator iterator = this.iterator(); iterator.hasNext();) {
                    final long next = iterator.nextLong();
                    final Object key = tupleSource.createTuple(next);
                    if (keys.contains(key)) {
                        resultBuilder
                            .computeIfAbsent(key, k -> Index.FACTORY.getSequentialBuilder())
                            .appendKey(next);
                    }
                }
                for (Map.Entry<Object, SequentialBuilder> objectIndexBuilderEntry : resultBuilder
                    .entrySet()) {
                    resultCollector.accept(objectIndexBuilderEntry.getKey(),
                        objectIndexBuilderEntry.getValue().getIndex());
                }
            }
        }
    }

    private static void generateGrouping(
        UnaryOperator<Index> indexOp,
        BiConsumer<Object, Index> resultCollector,
        TupleSource tupleSource,
        List<ColumnSource> keyColumns,
        int position,
        Object[] partialValues,
        Index partiallyIntersectedIndex) {
        for (Object objectEntry : keyColumns.get(position).getGroupToRange().entrySet()) {
            // noinspection unchecked
            final Map.Entry<Object, Index> entry = (Map.Entry<Object, Index>) objectEntry;
            partialValues[position] = entry.getKey();
            final Index subIndex;
            if (position == 0) {
                subIndex = indexOp.apply(entry.getValue());
            } else {
                subIndex = partiallyIntersectedIndex.intersect(entry.getValue());
            }
            if (subIndex.nonempty()) {
                if (position == keyColumns.size() - 1) {
                    // we're at the very last bit, so we should start shoving our tuples into the
                    // result map
                    resultCollector.accept(
                        tupleSource.createTupleFromReinterpretedValues(partialValues), subIndex);
                } else {
                    generateGrouping(indexOp, resultCollector, tupleSource, keyColumns,
                        position + 1, partialValues, subIndex);
                }
            }
        }
    }

    private void generateGrouping(BiConsumer<Object, Index> resultCollector,
        TupleSource tupleSource, List<ColumnSource> keyColumns, int position,
        Object[] partialValues, Index partiallyIntersectedIndex, Set<Object> keyRestriction) {
        final boolean finalPosition = position == keyColumns.size() - 1;

        final List<ColumnSource> subSources = keyColumns.subList(0, position + 1);
        final TupleSource subTupleSource = TupleSourceFactory
            .makeTupleSource(subSources.toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY));

        final Set<Object> pruningSet;
        if (finalPosition) {
            pruningSet = keyRestriction;
        } else if (position == 0) {
            // noinspection unchecked
            pruningSet =
                keyRestriction.stream().map(x -> tupleSource.exportElementReinterpreted(x, 0))
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
        final Map<Object, Index> groupToRange =
            (Map<Object, Index>) keyColumns.get(position).getGroupToRange();
        final Object[] pruningKey = Arrays.copyOf(partialValues, position + 1);
        for (Map.Entry<Object, Index> entry : groupToRange.entrySet()) {
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
                if (!pruningSet
                    .contains(subTupleSource.createTupleFromReinterpretedValues(pruningKey))) {
                    continue;
                }

                tuple = null;
            }

            final Index subIndex;
            if (position == 0) {
                subIndex = intersect(entry.getValue());
            } else {
                subIndex = partiallyIntersectedIndex.intersect(entry.getValue());
            }

            if (subIndex.nonempty()) {
                if (finalPosition) {
                    // we're at the very last bit, so we should start shoving our smart keys into
                    // the result map
                    resultCollector.accept(tuple, subIndex);
                } else {
                    generateGrouping(resultCollector, tupleSource, keyColumns, position + 1,
                        partialValues, subIndex, keyRestriction);
                }
            }
        }
    }

    @Override
    public Map<Object, Index> getPrevGrouping(TupleSource tupleSource) {
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();

        Map<Object, Index> result = lookupPrevMapping(sourcesKey);
        if (result != null) {
            return result;
        }
        result = new LinkedHashMap<>();
        if (sourcesKey.isEmpty()) {
            result.put(EmptyTuple.INSTANCE, this.clone());
        } else {
            final Map<Object, SequentialBuilder> resultBuilder = new LinkedHashMap<>();
            for (final Index.Iterator iterator = this.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();
                final Object key = tupleSource.createPreviousTuple(next);
                resultBuilder.computeIfAbsent(key, k -> Index.FACTORY.getSequentialBuilder())
                    .appendKey(next);
            }
            result = new LinkedHashMap<>();
            for (Map.Entry<Object, SequentialBuilder> objectIndexBuilderEntry : resultBuilder
                .entrySet()) {
                result.put(objectIndexBuilderEntry.getKey(),
                    objectIndexBuilderEntry.getValue().getIndex());
            }
        }
        if (areColumnsImmutable(sourcesKey)) {
            if (mappings == null) {
                mappings = new WeakHashMap<>();
            }
            mappings.put(sourcesKey, new MappingInfo(result, 0, tupleSource));
        } else {
            if (ephemeralPrevMappings == null) {
                ephemeralPrevMappings = new WeakHashMap<>();
            }
            ephemeralPrevMappings.put(sourcesKey,
                new MappingInfo(result, LogicalClock.DEFAULT.currentStep(), tupleSource));
        }
        return result;
    }

    @Override
    public boolean hasGrouping(ColumnSource... keyColumns) {
        if (keyColumns.length == 0) {
            return true;
        }
        final List<ColumnSource> sourcesKey = Arrays.asList(keyColumns);
        final Map<Object, Index> groupingCandidate = lookupMapping(sourcesKey);
        return groupingCandidate != null
            || keyColumns.length == 1 && keyColumns[0].getGroupToRange() != null;
    }

    private boolean areColumnsImmutable(List<ColumnSource> sourcesKey) {
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
