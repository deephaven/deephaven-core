/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.structures.rowsequence.RowSequenceAsChunkImpl;
import io.deephaven.engine.util.tuples.EmptyTuple;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.LogicalClock;
import io.deephaven.engine.v2.sources.chunk.Attributes.OrderedRowKeys;
import io.deephaven.engine.v2.sources.chunk.LongChunk;
import io.deephaven.engine.v2.tuples.TupleSource;
import io.deephaven.engine.v2.tuples.TupleSourceFactory;
import gnu.trove.list.array.TLongArrayList;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;

public abstract class GroupingRowSetHelper extends RowSequenceAsChunkImpl implements TrackingMutableRowSet {

    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneDoesntDeclareCloneNotSupportedException"})
    @Override
    public TrackingMutableRowSet clone() {
        throw new UnsupportedOperationException();
    }

    /**
     * These mappings last forever, and only include column sources that are immutable.
     *
     * Whenever this rowSet is changed, we must clear the mappings.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> mappings = null;

    private static class MappingInfo {
        private final TupleSource tupleSource;
        private final Map<Object, TrackingMutableRowSet> mapping;
        private final long creationTick;

        private MappingInfo(Map<Object, TrackingMutableRowSet> mapping, long creationTick, TupleSource tupleSource) {
            this.mapping = mapping;
            this.creationTick = creationTick;
            this.tupleSource = tupleSource;
        }
    }

    /**
     * These mappings do not survive past a single LogicalClock tick or any rowSet changes.
     */
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings = null;
    private transient WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralPrevMappings = null;

    protected final void onUpdate(final RowSet added, final RowSet removed) {
        clearMappings();
    }

    protected final void onRemove(final RowSet removed) {
        clearMappings();
    }

    protected final void onRetain(final RowSet intersected) {
        clearMappings();
    }

    protected final void onInsert(final RowSet added) {
        clearMappings();
    }

    protected final void onClear() {
        clearMappings();
    }

    /**
     * @return Whether this rowSet is flat (that is, contiguous from 0 to size - 1)
     */
    public final boolean isFlat() {
        return empty() || (lastRowKey() == size() - 1);
    }

    @Override
    public void insert(final RowSet added) {
        for (final TrackingMutableRowSet.RangeIterator iterator = added.rangeIterator(); iterator.hasNext();) {
            iterator.next();
            insertRange(iterator.currentRangeStart(), iterator.currentRangeEnd());
        }
    }

    @Override
    public void remove(RowSet removed) {
        for (final TrackingMutableRowSet.Iterator iterator = removed.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            remove(next);
        }
    }

    @Override
    public TrackingMutableRowSet invert(RowSet keys) {
        return invert(keys, Long.MAX_VALUE);
    }

    /**
     * The only used implementation of invert is in the TrackingMutableRowSetImpl, really the guts of it are in BspNodeIndex.
     *
     * This version is inefficient as it simply performs O(keys) find operations; which is O(keys * lg size), because
     * there is no memory about what you've already found.
     *
     * It serves as a reasonable reference for what the invert operation is "meant" to do.
     *
     * Note maximumPosition is inclusive.
     */
    @Override
    public TrackingMutableRowSet invert(final RowSet keys, final long maximumPosition) {
        final SequentialRowSetBuilder indexBuilder = TrackingMutableRowSet.FACTORY.getSequentialBuilder();
        for (final TrackingMutableRowSet.Iterator iterator = keys.iterator(); iterator.hasNext();) {
            final long next = iterator.nextLong();
            final long position = find(next);
            if (position > maximumPosition) {
                break;
            }
            Assert.geqZero(position, "position");
            indexBuilder.appendKey(position);
        }
        return indexBuilder.build();
    }

    @Override
    public TLongArrayList[] findMissing(RowSet keys) {
        return IndexUtilities.findMissing(this, keys);
    }

    private Map<Object, TrackingMutableRowSet> lookupMapping(List<ColumnSource> columnSourceKey) {
        return lookupMapping(mappings, ephemeralMappings, columnSourceKey);
    }

    private static Map<Object, TrackingMutableRowSet> lookupMapping(
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            List<ColumnSource> columnSourceKey) {
        final Map<Object, TrackingMutableRowSet> immutableMapping = lookupImmutableMapping(mappings, columnSourceKey);
        if (immutableMapping != null)
            return immutableMapping;
        return lookupEphemeralMapping(columnSourceKey, ephemeralMappings);
    }

    private static Map<Object, TrackingMutableRowSet> lookupPrevMapping(
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralPrevMappings,
            List<ColumnSource> columnSourceKey) {
        final Map<Object, TrackingMutableRowSet> immutableMapping = lookupImmutableMapping(mappings, columnSourceKey);
        if (immutableMapping != null)
            return immutableMapping;
        return lookupEphemeralMapping(columnSourceKey, ephemeralPrevMappings);
    }

    private Map<Object, TrackingMutableRowSet> lookupPrevMapping(List<ColumnSource> columnSourceKey) {
        return lookupPrevMapping(mappings, ephemeralPrevMappings, columnSourceKey);
    }

    private static Map<Object, TrackingMutableRowSet> lookupImmutableMapping(
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

    private static Map<Object, TrackingMutableRowSet> lookupEphemeralMapping(List<ColumnSource> columnSourceKey,
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
        for (TrackingMutableRowSet.Iterator it = this.iterator(); it.hasNext();) {
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
    protected void updateGroupingOnInsert(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
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
    protected void updateGroupingOnRemove(final LongChunk<OrderedRowKeys> keys, final int offset, final int length) {
        clearMappings();
    }

    protected void updateGroupingOnRetainRange(final long start, final long end) {
        clearMappings();
    }

    protected void updateGroupingOnRetain(final TrackingMutableRowSet intersected) {
        clearMappings();
    }

    public static Map<Object, TrackingMutableRowSet> getGrouping(
            final TrackingMutableRowSet thisRowSet,
            UnaryOperator<TrackingMutableRowSet> indexOp,
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();
        final Map<Object, TrackingMutableRowSet> lookupResult = lookupMapping(mappings, ephemeralMappings, sourcesKey);
        if (lookupResult != null) {
            return lookupResult;
        }

        final Map<Object, TrackingMutableRowSet> result = new LinkedHashMap<>();

        BiConsumer<Object, TrackingMutableRowSet> resultCollector = result::put;

        collectGrouping(thisRowSet, indexOp, mappings, ephemeralMappings, resultCollector, tupleSource, sourcesKey);

        return result;
    }

    @Override
    public Map<Object, TrackingMutableRowSet> getGrouping(TupleSource tupleSource) {
        // noinspection unchecked
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();
        final Map<Object, TrackingMutableRowSet> lookupResult = lookupMapping(sourcesKey);
        if (lookupResult != null) {
            return lookupResult;
        }

        final Map<Object, TrackingMutableRowSet> result = new LinkedHashMap<>();

        final BiConsumer<Object, TrackingMutableRowSet> resultCollector = result::put;

        collectGrouping(this, this::intersect, mappings, ephemeralMappings, resultCollector, tupleSource, sourcesKey);

        // TODO: We need to do something better than weakly-reachable List<ColumnSource>s here. Indices are probably
        // cleaned up well before we want right now. Values only cleaned up on access. Both are sub-par.
        if (areColumnsImmutable(sourcesKey)) {
            if (mappings == null) {
                mappings = new WeakHashMap<>();
            }
            mappings.put(sourcesKey, new MappingInfo(result, 0, tupleSource));
        } else {
            if (ephemeralMappings == null) {
                ephemeralMappings = new WeakHashMap<>();
            }
            ephemeralMappings.put(sourcesKey, new MappingInfo(result, LogicalClock.DEFAULT.currentStep(), tupleSource));
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
            final TrackingMutableRowSet thisRowSet,
            final UnaryOperator<TrackingMutableRowSet> indexOp,
            WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            final BiConsumer<Object, TrackingMutableRowSet> resultCollector,
            final TupleSource tupleSource,
            final List<ColumnSource> keyColumns) {
        if (keyColumns.isEmpty()) {
            resultCollector.accept(EmptyTuple.INSTANCE, thisRowSet.clone());
        } else if (keyColumns.size() == 1 && keyColumns.get(0).getGroupToRange() != null) {
            @SuppressWarnings("unchecked")
            final Map<Object, TrackingMutableRowSet> sourceGrouping = keyColumns.get(0).getGroupToRange();
            for (Map.Entry<Object, TrackingMutableRowSet> objectIndexEntry : sourceGrouping.entrySet()) {
                final TrackingMutableRowSet resultRowSet = indexOp.apply(objectIndexEntry.getValue());
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
                final Map<Object, SequentialRowSetBuilder> resultBuilder = new LinkedHashMap<>();
                for (final TrackingMutableRowSet.Iterator iterator = thisRowSet.iterator(); iterator.hasNext();) {
                    final long next = iterator.nextLong();
                    final Object key = tupleSource.createTuple(next);
                    resultBuilder.computeIfAbsent(key, k -> TrackingMutableRowSet.FACTORY.getSequentialBuilder()).appendKey(next);
                }
                resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.build()));
            }
        }
    }

    private static void generatePartialGrouping(
            final TrackingMutableRowSet thisRowSet,
            final UnaryOperator<TrackingMutableRowSet> indexOp,
            final WeakHashMap<List<ColumnSource>, MappingInfo> mappings,
            final WeakHashMap<List<ColumnSource>, MappingInfo> ephemeralMappings,
            final BiConsumer<Object, TrackingMutableRowSet> resultCollector, TupleSource tupleSource, List<ColumnSource> keyColumns) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        final TupleSource groupedTupleSource = TupleSourceFactory.makeTupleSource(groupedKeyColumns);
        final Map<Object, TrackingMutableRowSet> groupedColumnsGrouping =
                getGrouping(thisRowSet, indexOp, mappings, ephemeralMappings, groupedTupleSource);
        generatePartialGroupingSecondHalf(groupedKeyColumns, notGroupedKeyColumns, groupedTupleSource,
                groupedColumnsGrouping,
                resultCollector, tupleSource, keyColumns);
    }

    private void generatePartialGrouping(BiConsumer<Object, TrackingMutableRowSet> resultCollector, TupleSource tupleSource,
                                         List<ColumnSource> keyColumns) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        final TupleSource groupedTupleSource = TupleSourceFactory.makeTupleSource(groupedKeyColumns);
        final Map<Object, TrackingMutableRowSet> groupedColumnsGrouping = getGrouping(groupedTupleSource);
        generatePartialGroupingSecondHalf(groupedKeyColumns, notGroupedKeyColumns, groupedTupleSource,
                groupedColumnsGrouping,
                resultCollector, tupleSource, keyColumns);
    }

    private static void generatePartialGroupingSecondHalf(
            final ColumnSource[] groupedKeyColumns, final ColumnSource[] notGroupedKeyColumns,
            final TupleSource groupedTupleSource, final Map<Object, TrackingMutableRowSet> groupedColumnsGrouping,
            final BiConsumer<Object, TrackingMutableRowSet> resultCollector, final TupleSource tupleSource,
            final List<ColumnSource> keyColumns) {
        final Map<Object, SequentialRowSetBuilder> resultBuilder = new LinkedHashMap<>();

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

        for (Map.Entry<Object, TrackingMutableRowSet> entry : groupedColumnsGrouping.entrySet()) {
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

            final TrackingMutableRowSet resultRowSet = entry.getValue();
            for (final TrackingMutableRowSet.Iterator iterator = resultRowSet.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();

                for (int ii = 0; ii < notGroupedKeysIndices.length; ++ii) {
                    partialKeyValues[notGroupedKeysIndices[ii]] = notGroupedKeyColumns[ii].get(next);
                }

                resultBuilder.computeIfAbsent(tupleSource.createTupleFromReinterpretedValues(partialKeyValues),
                        k -> TrackingMutableRowSet.FACTORY.getSequentialBuilder()).appendKey(next);
            }
        }

        resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.build()));
    }

    private void generatePartialGroupingForKeySet(BiConsumer<Object, TrackingMutableRowSet> resultCollector, TupleSource tupleSource,
                                                  List<ColumnSource> keyColumns, Set<Object> keys) {
        // we can generate the grouping partially from our constituents
        final ColumnSource[] groupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() != null).toArray(ColumnSource[]::new);
        final ColumnSource[] notGroupedKeyColumns =
                keyColumns.stream().filter(cs -> cs.getGroupToRange() == null).toArray(ColumnSource[]::new);

        Require.gtZero(groupedKeyColumns.length, "groupedKeyColumns.length");
        Require.gtZero(notGroupedKeyColumns.length, "notGroupedKeyColumns.length");

        final TupleSource groupedTupleSource = TupleSourceFactory.makeTupleSource(groupedKeyColumns);

        final Map<Object, SequentialRowSetBuilder> resultBuilder = new LinkedHashMap<>();

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

        final Map<Object, TrackingMutableRowSet> groupedColumnsGrouping = getGroupingForKeySet(groupPruningSet, groupedTupleSource);

        final Object[] lookupKeyValues = new Object[keyColumns.size()];

        for (Map.Entry<Object, TrackingMutableRowSet> entry : groupedColumnsGrouping.entrySet()) {
            final TrackingMutableRowSet resultRowSet = entry.getValue().intersect(this);
            if (resultRowSet.empty()) {
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

            for (final TrackingMutableRowSet.Iterator iterator = resultRowSet.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();

                for (int ii = 0; ii < notGroupedKeysIndices.length; ++ii) {
                    lookupKeyValues[notGroupedKeysIndices[ii]] = notGroupedKeyColumns[ii].get(next);
                }

                final Object key = tupleSource.createTupleFromReinterpretedValues(lookupKeyValues);
                if (!keys.contains(key)) {
                    continue;
                }

                final SequentialRowSetBuilder indexForKey =
                        resultBuilder.computeIfAbsent(key, k -> TrackingMutableRowSet.FACTORY.getSequentialBuilder());
                indexForKey.appendKey(next);
            }
        }

        resultBuilder.forEach((k, v) -> resultCollector.accept(k, v.build()));
    }

    @Override
    public Map<Object, TrackingMutableRowSet> getGroupingForKeySet(Set<Object> keys, TupleSource tupleSource) {
        final Map<Object, TrackingMutableRowSet> result = new LinkedHashMap<>();

        collectGroupingForKeySet(keys, tupleSource, result::put);

        return result;
    }

    @Override
    public TrackingMutableRowSet getSubIndexForKeySet(Set<Object> keys, TupleSource tupleSource) {
        final RowSetBuilder rowSetBuilder = TrackingMutableRowSet.FACTORY.getBuilder();
        final BiConsumer<Object, TrackingMutableRowSet> resultCollector = (key, index) -> rowSetBuilder.addRowSet(index);

        collectGroupingForKeySet(keys, tupleSource, resultCollector);

        return rowSetBuilder.build();
    }

    private void collectGroupingForKeySet(Set<Object> keys, TupleSource tupleSource,
            BiConsumer<Object, TrackingMutableRowSet> resultCollector) {
        // noinspection unchecked
        final List<ColumnSource> keyColumns = tupleSource.getColumnSources();
        if (keyColumns.isEmpty()) {
            resultCollector.accept(EmptyTuple.INSTANCE, this.clone());
        } else if (keyColumns.size() == 1 && keyColumns.get(0).getGroupToRange() != null) {
            @SuppressWarnings("unchecked")
            final Map<Object, TrackingMutableRowSet> sourceGrouping = keyColumns.get(0).getGroupToRange();
            sourceGrouping.entrySet().stream().filter(objectIndexEntry -> keys.contains(objectIndexEntry.getKey()))
                    .forEach(objectIndexEntry -> {
                        final TrackingMutableRowSet resultRowSet = objectIndexEntry.getValue().intersect(this);
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
                final Map<Object, SequentialRowSetBuilder> resultBuilder = new LinkedHashMap<>();
                for (final TrackingMutableRowSet.Iterator iterator = this.iterator(); iterator.hasNext();) {
                    final long next = iterator.nextLong();
                    final Object key = tupleSource.createTuple(next);
                    if (keys.contains(key)) {
                        resultBuilder.computeIfAbsent(key, k -> TrackingMutableRowSet.FACTORY.getSequentialBuilder()).appendKey(next);
                    }
                }
                for (Map.Entry<Object, SequentialRowSetBuilder> objectIndexBuilderEntry : resultBuilder.entrySet()) {
                    resultCollector.accept(objectIndexBuilderEntry.getKey(),
                            objectIndexBuilderEntry.getValue().build());
                }
            }
        }
    }

    private static void generateGrouping(
            UnaryOperator<TrackingMutableRowSet> indexOp,
            BiConsumer<Object, TrackingMutableRowSet> resultCollector,
            TupleSource tupleSource,
            List<ColumnSource> keyColumns,
            int position,
            Object[] partialValues,
            TrackingMutableRowSet partiallyIntersectedRowSet) {
        for (Object objectEntry : keyColumns.get(position).getGroupToRange().entrySet()) {
            // noinspection unchecked
            final Map.Entry<Object, TrackingMutableRowSet> entry = (Map.Entry<Object, TrackingMutableRowSet>) objectEntry;
            partialValues[position] = entry.getKey();
            final TrackingMutableRowSet subRowSet;
            if (position == 0) {
                subRowSet = indexOp.apply(entry.getValue());
            } else {
                subRowSet = partiallyIntersectedRowSet.intersect(entry.getValue());
            }
            if (subRowSet.nonempty()) {
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

    private void generateGrouping(BiConsumer<Object, TrackingMutableRowSet> resultCollector, TupleSource tupleSource,
                                  List<ColumnSource> keyColumns, int position, Object[] partialValues, TrackingMutableRowSet partiallyIntersectedRowSet,
                                  Set<Object> keyRestriction) {
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
        final Map<Object, TrackingMutableRowSet> groupToRange = (Map<Object, TrackingMutableRowSet>) keyColumns.get(position).getGroupToRange();
        final Object[] pruningKey = Arrays.copyOf(partialValues, position + 1);
        for (Map.Entry<Object, TrackingMutableRowSet> entry : groupToRange.entrySet()) {
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

            final TrackingMutableRowSet subRowSet;
            if (position == 0) {
                subRowSet = intersect(entry.getValue());
            } else {
                subRowSet = partiallyIntersectedRowSet.intersect(entry.getValue());
            }

            if (subRowSet.nonempty()) {
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
    public Map<Object, TrackingMutableRowSet> getPrevGrouping(TupleSource tupleSource) {
        final List<ColumnSource> sourcesKey = tupleSource.getColumnSources();

        Map<Object, TrackingMutableRowSet> result = lookupPrevMapping(sourcesKey);
        if (result != null) {
            return result;
        }
        result = new LinkedHashMap<>();
        if (sourcesKey.isEmpty()) {
            result.put(EmptyTuple.INSTANCE, this.clone());
        } else {
            final Map<Object, SequentialRowSetBuilder> resultBuilder = new LinkedHashMap<>();
            for (final TrackingMutableRowSet.Iterator iterator = this.iterator(); iterator.hasNext();) {
                final long next = iterator.nextLong();
                final Object key = tupleSource.createPreviousTuple(next);
                resultBuilder.computeIfAbsent(key, k -> TrackingMutableRowSet.FACTORY.getSequentialBuilder()).appendKey(next);
            }
            result = new LinkedHashMap<>();
            for (Map.Entry<Object, SequentialRowSetBuilder> objectIndexBuilderEntry : resultBuilder.entrySet()) {
                result.put(objectIndexBuilderEntry.getKey(), objectIndexBuilderEntry.getValue().build());
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
        final Map<Object, TrackingMutableRowSet> groupingCandidate = lookupMapping(sourcesKey);
        return groupingCandidate != null || keyColumns.length == 1 && keyColumns[0].getGroupToRange() != null;
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
