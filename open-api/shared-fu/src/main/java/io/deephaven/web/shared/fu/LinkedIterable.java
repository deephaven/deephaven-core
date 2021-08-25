package io.deephaven.web.shared.fu;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.UnaryOperator;

public class LinkedIterable<T> implements MappedIterable<T> {

    private final class Itr implements Iterator<T> {

        T node, prev;
        boolean computed, removed;

        @Override
        public boolean hasNext() {
            if (!computed) {
                computed = true;
                final T newNode = next.apply(node);
                if (node == newNode) {
                    return false;
                }
                prev = node;
                node = newNode;
            }
            return node != null;
        }

        @Override
        public T next() {
            removed = false;
            if (!computed) {
                hasNext();
            }
            computed = false;
            if (node == null) {
                throw new NoSuchElementException();
            }
            return node;
        }

        @Override
        public void remove() {
            if (remover != null) {
                if (removed) {
                    throw new IllegalStateException("Cannot call remove() more than once per next()");
                }
                removed = true;
                if (node == null) {
                    throw new IllegalStateException("Cannot remove() before next()");
                }
                remover.apply(prev, node);
            } else {
                // unsupported
                Iterator.super.remove();
            }
        }
    }

    private final T head;
    private final UnaryOperator<T> next;
    private final boolean skipHead;
    private JsBiConsumer<T, T> remover;

    public LinkedIterable(T head, T tail, boolean includeTail, boolean strict, UnaryOperator<T> next) {
        this(head, new UnaryOperator<T>() {
            boolean done = head == tail; // when head == tail, we will always return the head anyway, so just skip the
                                         // next-ing.

            @Override
            public T apply(T cur) {
                if (done) {
                    return null;
                }
                if (cur == tail) {
                    if (!includeTail) {
                        return null;
                    }
                    return cur;
                }
                final T find = next.apply(cur);
                if (strict && find == null) {
                    throw new IllegalStateException("Iterable starting at " + head + " did not end with " + tail);
                }
                if (find == tail) {
                    done = true;
                }
                return find;
            }
        }, false);
        if (strict) {
            if (head == null) {
                throw new NullPointerException("iterable start cannot be null!");
            }
            if (tail == null) {
                throw new NullPointerException("iterable end cannot be null!");
            }
        }
    }

    public LinkedIterable(T head, UnaryOperator<T> next) {
        this(head, next, false);
    }

    public LinkedIterable(T head, UnaryOperator<T> next, boolean skipHead) {
        this.head = head;
        this.next = next;
        this.skipHead = skipHead;
    }

    @Override
    public Iterator<T> iterator() {
        final Itr itr = new Itr();
        itr.node = head;
        itr.computed = !skipHead;
        return itr;
    }

    public LinkedIterable<T> setRemover(JsBiConsumer<T, T> remover) {
        this.remover = remover;
        return this;
    }
}
