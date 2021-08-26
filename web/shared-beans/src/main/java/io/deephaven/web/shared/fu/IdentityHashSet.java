package io.deephaven.web.shared.fu;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;

/**
 * A set that internally wraps an IdentityHashMap.
 *
 * Should be very fast for pooled objects, and completely wrong for different instances of Object
 * which .equal() each other.
 *
 * Use only if you can guarantee type parameter T is a pooled object (TableHandle, JsTable,
 * ClientTableState, etc.)
 */
public class IdentityHashSet<T> extends AbstractSet<T> {

    private final IdentityHashMap<T, T> map = new IdentityHashMap<>();

    public IdentityHashSet() {}

    public IdentityHashSet(Collection<T> from) {
        addAll(from);
    }

    @Override
    public Iterator<T> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean add(T t) {
        return map.put(t, t) != null;
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) != null;
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }
}
