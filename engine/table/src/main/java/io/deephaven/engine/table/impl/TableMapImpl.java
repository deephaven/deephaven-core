/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.datastructures.hash.IdentityKeyedObjectKey;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

public abstract class TableMapImpl extends LivenessArtifact implements TableMap, DynamicNode {

    private final WeakReferenceManager<Listener> listeners = new WeakReferenceManager<>(true);
    private final WeakReferenceManager<KeyListener> keyListeners = new WeakReferenceManager<>(true);

    @ReferentialIntegrity
    private final Collection<Object> parents = new KeyedObjectHashSet<>(IdentityKeyedObjectKey.getInstance());
    private boolean refreshing;

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    @Override
    public void addKeyListener(KeyListener listener) {
        keyListeners.add(listener);
    }

    @Override
    public void removeKeyListener(KeyListener listener) {
        keyListeners.remove(listener);
    }

    /**
     * Notify {@link TableMap.Listener}s of an inserted table.
     *
     * @param key the newly inserted key
     * @param table the corresponding table
     */
    protected void notifyListeners(Object key, Table table) {
        listeners.forEachValidReference(l -> l.handleTableAdded(key, table));
    }

    /**
     * Notify {@link TableMap.KeyListener}s of an inserted key.
     *
     * @param key the newly inserted key
     */
    protected void notifyKeyListeners(Object key) {
        keyListeners.forEachValidReference(l -> l.handleKeyAdded(key));
    }

    /**
     * Returns true if there are any {@link TableMap.Listener} for table additions.
     *
     * <p>
     * Note that this function returns false if there are only KeyListeners.
     * </p>
     *
     * @return true if there are any Listeners.
     */
    boolean hasListeners() {
        return !listeners.isEmpty();
    }

    @Override
    public boolean isRefreshing() {
        return refreshing;
    }

    @Override
    public boolean setRefreshing(final boolean refreshing) {
        return this.refreshing = refreshing;
    }

    @Override
    public void addParentReference(@NotNull final Object parent) {
        if (DynamicNode.notDynamicOrIsRefreshing(parent)) {
            setRefreshing(true);
            parents.add(parent);
            if (parent instanceof LivenessReferent) {
                manage((LivenessReferent) parent);
            }
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        listeners.clear();
        keyListeners.clear();
        parents.clear();
    }
}
