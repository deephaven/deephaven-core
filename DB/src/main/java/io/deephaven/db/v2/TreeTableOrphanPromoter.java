package io.deephaven.db.v2;

import io.deephaven.base.Function;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.remote.WrappedDelegatingTable;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.util.QueryConstants;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.stream.Stream;

import static io.deephaven.db.tables.Table.TREE_TABLE_FILTER_REVERSE_LOOKUP_ATTRIBUTE;

/**
 * <p>
 * Identify orphan rows in a table destined for conversion into a tree table, and mask their parent
 * column value to null, so that they show up at the top level of the hierarchy.
 * </p>
 *
 * <p>
 * This is useful if your data contains values which you can not identify as top-level rows; or if
 * you would like to filter your tree table source, excluding parents which do not meet your filter
 * criteria, but do not want to orphan the matches.
 * </p>
 *
 * <p>
 * This class should be used by calling {@link #promoteOrphans(Table, String, String)} method.
 * </p>
 */
public class TreeTableOrphanPromoter implements Function.Unary<Table, Table> {
    private final String idColumn;
    private final String parentColumn;

    private TreeTableOrphanPromoter(String idColumn, String parentColumn) {
        this.idColumn = idColumn;
        this.parentColumn = parentColumn;
    }

    @WrappedDelegatingTable.DoNotWrap
    @Override
    public Table call(Table table) {
        if (table.isLive()) {
            LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        }

        return new State(table).invoke();
    }

    private class State {
        private final ColumnSource parentSource;
        private final ColumnSource idSource;
        private final ReverseLookup reverseLookupListener;
        private final DynamicTable source;

        public State(Table table) {
            source = (DynamicTable) table;
            reverseLookupListener = getReverseLookupListener(source, idColumn);
            parentSource = source.getColumnSource(parentColumn);
            idSource = source.getColumnSource(idColumn);
        }

        public Table invoke() {
            final Map<String, ColumnSource> nameToColumns =
                new LinkedHashMap<>(source.getColumnSourceMap());

            // noinspection unchecked
            final ColumnSource parentView =
                new AbstractColumnSource.DefaultedMutable(parentSource.getType()) {
                    @Override
                    public Object get(long index) {
                        if (hasParent(index)) {
                            return parentSource.get(index);
                        }
                        return null;
                    }

                    @Override
                    public Object getPrev(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrev(index);
                        }
                        return null;
                    }

                    @Override
                    public Boolean getPrevBoolean(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevBoolean(index);
                        }
                        return null;
                    }

                    @Override
                    public byte getPrevByte(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevByte(index);
                        }
                        return QueryConstants.NULL_BYTE;
                    }

                    @Override
                    public char getPrevChar(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevChar(index);
                        }
                        return QueryConstants.NULL_CHAR;
                    }

                    @Override
                    public double getPrevDouble(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevDouble(index);
                        }
                        return QueryConstants.NULL_DOUBLE;
                    }

                    @Override
                    public float getPrevFloat(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevFloat(index);
                        }
                        return QueryConstants.NULL_FLOAT;
                    }

                    @Override
                    public int getPrevInt(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevInt(index);
                        }
                        return QueryConstants.NULL_INT;
                    }

                    @Override
                    public long getPrevLong(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevLong(index);
                        }
                        return QueryConstants.NULL_LONG;
                    }

                    @Override
                    public short getPrevShort(long index) {
                        if (hadParent(index)) {
                            return parentSource.getPrevShort(index);
                        }
                        return QueryConstants.NULL_SHORT;
                    }

                    @Override
                    public Boolean getBoolean(long index) {
                        if (hasParent(index)) {
                            return parentSource.getBoolean(index);
                        }
                        return null;
                    }

                    @Override
                    public byte getByte(long index) {
                        if (hasParent(index)) {
                            return parentSource.getByte(index);
                        }
                        return QueryConstants.NULL_BYTE;
                    }

                    @Override
                    public char getChar(long index) {
                        if (hasParent(index)) {
                            return parentSource.getChar(index);
                        }
                        return QueryConstants.NULL_CHAR;
                    }

                    @Override
                    public double getDouble(long index) {
                        if (hasParent(index)) {
                            return parentSource.getDouble(index);
                        }
                        return QueryConstants.NULL_DOUBLE;
                    }

                    @Override
                    public float getFloat(long index) {
                        if (hasParent(index)) {
                            return parentSource.getFloat(index);
                        }
                        return QueryConstants.NULL_FLOAT;

                    }

                    @Override
                    public int getInt(long index) {
                        if (hasParent(index)) {
                            return parentSource.getInt(index);
                        }
                        return QueryConstants.NULL_INT;

                    }

                    @Override
                    public long getLong(long index) {
                        if (hasParent(index)) {
                            return parentSource.getLong(index);
                        }
                        return QueryConstants.NULL_LONG;
                    }

                    @Override
                    public short getShort(long index) {
                        if (hasParent(index)) {
                            return parentSource.getShort(index);
                        }
                        return QueryConstants.NULL_SHORT;
                    }
                };

            nameToColumns.put(parentColumn, parentView);

            final QueryTable result = new QueryTable(source.getIndex(), nameToColumns);


            if (source.isRefreshing()) {
                result.addParentReference(reverseLookupListener);

                final ModifiedColumnSet inputColumns =
                    source.newModifiedColumnSet(idColumn, parentColumn);

                final String[] columnNames = source.getColumnSourceMap().keySet()
                    .toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                final ModifiedColumnSet.Transformer mcsTransformer =
                    source.newModifiedColumnSetTransformer(result, columnNames);
                final ModifiedColumnSet mcsParentColumn = result.newModifiedColumnSet(parentColumn);

                final ShiftAwareListener listener =
                    new BaseTable.ShiftAwareListenerImpl("Orphan Promoter", source, result) {
                        final Map<Object, TLongSet> parentToChildMap = new HashMap<>();

                        {
                            addChildren(source.getIndex());
                        }

                        private void addChildren(Index index) {
                            for (final Index.Iterator it = index.iterator(); it.hasNext();) {
                                final long key = it.nextLong();
                                final Object parent = parentSource.get(key);
                                if (parent != null) {
                                    parentToChildMap
                                        .computeIfAbsent(parent, x -> new TLongHashSet()).add(key);
                                }
                            }
                        }

                        private void removeChildren(Index index) {
                            for (final Index.Iterator it = index.iterator(); it.hasNext();) {
                                final long key = it.nextLong();
                                final Object oldParent = parentSource.getPrev(key);
                                if (oldParent != null) {
                                    removeFromParent(oldParent, parentToChildMap.get(oldParent),
                                        key);
                                }
                            }
                        }

                        private void removeFromParent(final Object oldParent,
                            final TLongSet oldParentSet, final long keyToRemove) {
                            if (oldParentSet == null) {
                                throw new IllegalStateException(
                                    "Could not find set for parent: " + oldParent);
                            }
                            if (!oldParentSet.remove(keyToRemove)) {
                                throw new IllegalStateException("key=" + keyToRemove
                                    + " was not in parent=" + oldParent + " set=" + oldParentSet);
                            }
                        }

                        @Override
                        public void onUpdate(final Update upstream) {
                            final Update downstream = upstream.copy();
                            downstream.modifiedColumnSet = result.modifiedColumnSet;

                            final boolean modifiedInputColumns =
                                upstream.modifiedColumnSet.containsAny(inputColumns);
                            if (upstream.added.empty() && upstream.removed.empty()
                                && upstream.shifted.empty() && !modifiedInputColumns) {
                                mcsTransformer.clearAndTransform(upstream.modifiedColumnSet,
                                    downstream.modifiedColumnSet);
                                result.notifyListeners(downstream);
                                return;
                            }

                            // Collect removed / added parent objects.
                            final Set<Object> removedIds = new HashSet<>();
                            final Set<Object> addedIds = new HashSet<>();

                            upstream.removed.forAllLongs((final long v) -> {
                                final Object id = idSource.getPrev(v);
                                removedIds.add(id);
                            });

                            upstream.added.forAllLongs((final long v) -> {
                                final Object id = idSource.get(v);
                                if (!removedIds.remove(id)) {
                                    addedIds.add(id);
                                }
                            });

                            if (modifiedInputColumns) {
                                // account for any rows with modified ids
                                upstream.forAllModified((preIndex, postIndex) -> {
                                    final Object prevId = idSource.getPrev(preIndex);
                                    final Object id = idSource.get(postIndex);
                                    if (!Objects.equals(id, prevId)) {
                                        if (!addedIds.contains(prevId)) {
                                            removedIds.add(prevId);
                                        }
                                        removedIds.remove(id);
                                        addedIds.add(id);
                                    }
                                });
                            }

                            // Process upstream changes and modify our state.
                            removeChildren(upstream.removed);
                            if (modifiedInputColumns) {
                                removeChildren(upstream.getModifiedPreShift());
                            }

                            try (final Index prevIndex = source.getIndex().getPrevIndex()) {
                                prevIndex.remove(upstream.removed);
                                if (modifiedInputColumns) {
                                    prevIndex.remove(upstream.getModifiedPreShift());
                                }

                                upstream.shifted.forAllInIndex(prevIndex, (key, shiftDelta) -> {
                                    final Object oldParent = parentSource.getPrev(key);
                                    final Object newParent = parentSource.get(key + shiftDelta);
                                    if (oldParent != null && Objects.equals(oldParent, newParent)) {
                                        final TLongSet set = parentToChildMap.get(oldParent);
                                        removeFromParent(oldParent, set, key);
                                        set.add(key + shiftDelta);
                                    } else {
                                        if (oldParent != null) {
                                            removeFromParent(oldParent,
                                                parentToChildMap.get(oldParent), key);
                                        }
                                        if (newParent != null) {
                                            parentToChildMap
                                                .computeIfAbsent(newParent, x -> new TLongHashSet())
                                                .add(key + shiftDelta);
                                        }
                                    }
                                });
                            }

                            if (modifiedInputColumns) {
                                addChildren(upstream.modified);
                            }
                            addChildren(upstream.added);

                            final TLongList modifiedKeys = new TLongArrayList();
                            Stream.concat(removedIds.stream(), addedIds.stream())
                                .map(parentToChildMap::get).filter(Objects::nonNull)
                                .forEach(x -> x.forEach(value -> {
                                    modifiedKeys.add(value);
                                    return true;
                                }));
                            modifiedKeys.sort();

                            final Index.SequentialBuilder builder =
                                Index.FACTORY.getSequentialBuilder();
                            // TODO: Modify this such that we don't actually ever add the keys to
                            // the builder if they exist
                            // within added; this would be made easier/more efficient if
                            // Index.Iterator exposed the
                            // advance() operation.
                            modifiedKeys.forEach(x -> {
                                builder.appendKey(x);
                                return true;
                            });

                            downstream.modified.insert(builder.getIndex());
                            downstream.modified.remove(upstream.added);

                            if (downstream.modified.nonempty()) {
                                mcsTransformer.clearAndTransform(upstream.modifiedColumnSet,
                                    downstream.modifiedColumnSet);
                                if (!modifiedKeys.isEmpty()) {
                                    downstream.modifiedColumnSet.setAll(mcsParentColumn);
                                }
                            } else {
                                downstream.modifiedColumnSet.clear();
                            }

                            result.notifyListeners(downstream);
                        }
                    };

                source.listenForUpdates(listener);
            }

            return result;
        }

        private boolean hasParent(long key) {
            final Object parentKey = parentSource.get(key);
            return reverseLookupListener.get(parentKey) != reverseLookupListener.getNoEntryValue();
        }

        private boolean hadParent(long key) {
            final Object parentKey = parentSource.getPrev(key);
            return reverseLookupListener.getPrev(parentKey) != reverseLookupListener
                .getNoEntryValue();
        }
    }

    /**
     * Convert a table with orphans to one without.
     *
     * @param table the input table to operate on
     * @param idColumn the column containing each row's unique ID
     * @param parentColumn the column containing the parent for this row; null for top-level rows
     *
     * @return a table where parentColumn is null if the original parent did not appear in the IDs
     */
    public static Table promoteOrphans(Table table, String idColumn, String parentColumn) {
        return table.apply(new TreeTableOrphanPromoter(idColumn, parentColumn));
    }

    static ReverseLookup getReverseLookupListener(DynamicTable source, String idColumn) {
        // noinspection unchecked
        Map<String, WeakReference<ReverseLookup>> rllMap =
            (Map<String, WeakReference<ReverseLookup>>) source
                .getAttribute(TREE_TABLE_FILTER_REVERSE_LOOKUP_ATTRIBUTE);
        if (rllMap == null) {
            rllMap = new HashMap<>();
            source.setAttribute(TREE_TABLE_FILTER_REVERSE_LOOKUP_ATTRIBUTE, rllMap);
        }
        final WeakReference<ReverseLookup> savedRll = rllMap.get(idColumn);
        final ReverseLookup cachedRll;
        if (savedRll != null && (cachedRll = savedRll.get()) != null) {
            return cachedRll;
        }

        final ReverseLookupListener result =
            ReverseLookupListener.makeReverseLookupListenerWithLock(source, idColumn);

        rllMap.put(idColumn, new WeakReference<>(result));
        return result;
    }
}
