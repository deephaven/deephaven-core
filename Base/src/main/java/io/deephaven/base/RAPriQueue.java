/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.Arrays;

/**
 * A "random-access" priority queue.
 */
@SuppressWarnings("unchecked")
public class RAPriQueue<T> {

    /** the queue adapter */
    public interface Adapter<T> {
        // return true if a is less than b, that is, a should be dequeued before b
        boolean less(T a, T b);

        // update the position of el in the priority queue
        void setPos(T el, int pos);

        // return the position of el in the priority queue; zero-or-negative if never set
        int getPos(T el);
    }

    /** the queue storage */
    private T[] queue;

    /** the queue adapter instance */
    private final Adapter<? super T> adapter;

    /** the class of objects we are queueing */
    private final Class<? super T> elementClass;

    /** the size of the queue (invariant: size < queue.length - 1) */
    private int size = 0;

    public RAPriQueue(int initialSize, Adapter<? super T> adapter, Class<? super T> elementClass) {
        this.queue = (T[]) java.lang.reflect.Array.newInstance(elementClass, initialSize + 1);
        this.adapter = adapter;
        this.elementClass = elementClass;
    }

    /** return the priority queue's size */
    public int size() {
        return size;
    }

    /** Returns true if the priority queue contains no elements. */
    public boolean isEmpty() {
        return size == 0;
    }

    /** clear out the queue */
    public void clear() {
        Arrays.fill(queue, null);
        size = 0;
    }

    /** Adds an element to the queue, or moves to appropriate place if already there */
    public void enter(T el) {
        int k = adapter.getPos(el);
        if (k <= 0) {
            if (++size == queue.length) {
                T[] newQueue = (T[]) java.lang.reflect.Array.newInstance(elementClass, 2 * queue.length);
                System.arraycopy(queue, 0, newQueue, 0, size);
                queue = newQueue;
            }
            queue[size] = el;
            adapter.setPos(el, size);
            fixUp(size);
            // assert testInvariant("after fixUp in enter-add");
        } else {
            assert queue[k] == el;
            fixDown(k);
            fixUp(k);
            // assert testInvariant("after fixDown/fixUp in enter-change");
        }
    }

    /** Return the top of the queue */
    public T top() {
        return queue[1];
    }

    /** Remove the top element from the queue, or null if the queue is empty */
    public T removeTop() {
        T el = queue[1];
        if (el != null) {
            adapter.setPos(el, 0);
            if (--size == 0) {
                queue[1] = null;
            } else {
                queue[1] = queue[size + 1];
                queue[size + 1] = null; // Drop extra reference to prevent memory leak
                adapter.setPos(queue[1], 1);
                fixDown(1);
            }
            // assert testInvariant("after removeTop()");
        }
        return el;
    }

    /** remove an arbitrary element from the queue */
    public void remove(T el) {
        int k = adapter.getPos(el);
        if (k != 0) {
            assert queue[k] == el;
            adapter.setPos(el, 0);
            if (k == size) {
                queue[size--] = null;
            } else {
                queue[k] = queue[size];
                adapter.setPos(queue[k], k);
                queue[size--] = null;
                fixDown(k);
                fixUp(k);
                // assert testInvariant("after fixDown/fixUp in remove()");
            }
        }
        // assert testInvariant("at end of remove()");
    }

    /** move queue[k] up the heap until it compares >= that of its parent. */
    private void fixUp(int k) {
        if (k > 1) {
            T el = queue[k];
            int j = k >> 1;
            T parent = queue[j];
            if (adapter.less(el, parent)) {
                queue[k] = parent;
                adapter.setPos(parent, k);
                k = j;
                j = k >> 1;
                while (k > 1 && adapter.less(el, parent = queue[j])) {
                    queue[k] = parent;
                    adapter.setPos(parent, k);
                    k = j;
                    j = k >> 1;
                }
                queue[k] = el;
                adapter.setPos(el, k);
            }
        }
    }

    /** move queue[k] down the heap until it compares <= those of its children. */
    private void fixDown(int k) {
        int j = k << 1;
        if (j <= size) {
            T el = queue[k], child = queue[j], child2;
            if (j < size && adapter.less(child2 = queue[j + 1], child)) {
                child = child2;
                j++;
            }
            if (adapter.less(child, el)) {
                queue[k] = child;
                adapter.setPos(child, k);
                k = j;
                j = k << 1;
                while (j <= size) {
                    child = queue[j];
                    if (j < size && adapter.less(child2 = queue[j + 1], child)) {
                        child = child2;
                        j++;
                    }
                    if (!adapter.less(child, el)) {
                        break;
                    }
                    queue[k] = child;
                    adapter.setPos(child, k);
                    k = j;
                    j = k << 1;
                }
                queue[k] = el;
                adapter.setPos(el, k);
            }
        }
    }

    public int dump(T[] result, int startIndex) {
        for (int i = 0; i < size; i++) {
            result[startIndex++] = queue[i + 1];
        }
        return startIndex;
    }

    public T get(int index) { // index is the "standard" 0..size-1
        return (index < size) ? queue[index + 1] : null;
    }

    public <T2> int dump(T2[] result, int startIndex, Function.Unary<T2, T> f) {
        for (int i = 0; i < size; i++) {
            result[startIndex++] = f.call(queue[i + 1]);
        }
        return startIndex;
    }

    boolean testInvariantAux(int i, String what) {
        if (i <= size) {
            if (adapter.getPos(queue[i]) != i) {
                System.err.println(what + ": queue[" + i + "].tqPos=" + (adapter.getPos(queue[i])) + " != " + i);
            }
            if (!testInvariantAux(i * 2, what)) {
                return false;
            }
            if (!testInvariantAux(i * 2 + 1, what)) {
                return false;
            }
            if (i > 1) {
                if (adapter.less(queue[i], queue[i / 2])) {
                    System.err.println(
                            what + ": child[" + i + "]=" + queue[i] + " < parent[" + (i / 2) + "]=" + queue[i / 2]);
                    return false;
                }
            }
        }
        return true;
    }

    boolean testInvariant(String what) {
        boolean result = testInvariantAux(1, what);
        if (result) {
            for (int i = size + 1; i < queue.length; ++i) {
                if (queue[i] != null) {
                    System.err.println(what + ": size = " + size + ", child[" + i + "]=" + queue[i] + " != null");
                    result = false;
                }
            }
        }
        return result;
    }
}
