/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.Assert;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.NoSuchElementException;

// --------------------------------------------------------------------
/**
 * This is a special version of {@link ArrayList} that can be substituted for a regular Array list but produces no
 * garbage. It only has one iterator, which is reused. It is not thread safe.
 */
public class LowGarbageArrayList<E> extends ArrayList<E> {

    private final Itr m_itr = new Itr();

    // ----------------------------------------------------------------
    @Override // from AbstractList
    public Iterator<E> iterator() {
        return m_itr.reset();
    }

    // ----------------------------------------------------------------
    @Override // from AbstractList
    public ListIterator<E> listIterator() {
        throw Assert.statementNeverExecuted("Not yet implemented.");
    }

    // ----------------------------------------------------------------
    @Override // from AbstractList
    public ListIterator<E> listIterator(int index) {
        throw Assert.statementNeverExecuted("Not yet implemented.");
    }

    // ################################################################

    // ----------------------------------------------------------------
    private class Itr implements Iterator<E> {
        /**
         * Index of element to be returned by subsequent call to next.
         */
        int cursor;

        /**
         * Index of element returned by most recent call to next or previous. Reset to -1 if this element is deleted by
         * a call to remove.
         */
        int lastRet;

        /**
         * The modCount value that the iterator believes that the backing List should have. If this expectation is
         * violated, the iterator has detected concurrent modification.
         */
        int expectedModCount;

        private Itr reset() {
            cursor = 0;
            lastRet = -1;
            expectedModCount = modCount;
            return this;
        }

        public boolean hasNext() {
            return cursor != size();
        }

        public E next() {
            checkForCoModification();
            try {
                E next = get(cursor);
                lastRet = cursor++;
                return next;
            } catch (IndexOutOfBoundsException e) {
                checkForCoModification();
                throw new NoSuchElementException();
            }
        }

        public void remove() {
            if (lastRet == -1)
                throw new IllegalStateException();
            checkForCoModification();

            try {
                LowGarbageArrayList.this.remove(lastRet);
                if (lastRet < cursor)
                    cursor--;
                lastRet = -1;
                expectedModCount = modCount;
            } catch (IndexOutOfBoundsException e) {
                throw new ConcurrentModificationException();
            }
        }

        final void checkForCoModification() {
            if (modCount != expectedModCount)
                throw new ConcurrentModificationException();
        }
    }

}
