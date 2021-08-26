/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import java.util.AbstractSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

// --------------------------------------------------------------------
/**
 * A very simple {@link Set} for small sets (uses linear time algorithms) that creates no garbage (except when
 * expanding). This set only has one {@link Iterator}, which is reused. This set is not thread safe.
 * <P>
 * Note: This class extends {@link HashSet} rather than {@link Set} (or {@link AbstractSet}) only because one of the
 * fields where we want to use it ({@link sun.nio.ch.SelectorImpl#keys}) is (improperly) declared as a HashSet rather
 * than a Set.
 */
public class LowGarbageArraySet<T> extends HashSet<T> {

    private T[] m_elements;
    private int m_nElements;

    private final MyIterator m_iterator = new MyIterator();

    // ----------------------------------------------------------------
    public LowGarbageArraySet() {
        super(0);
        // noinspection unchecked
        m_elements = (T[]) new Object[16];
    }

    // ----------------------------------------------------------------
    private LowGarbageArraySet(T[] elements, int nElements) {
        super(0);
        m_elements = elements;
        m_nElements = nElements;
    }

    // note: methods here are defined in the same order as HashSet.

    // ----------------------------------------------------------------
    @Override
    public Iterator<T> iterator() {
        m_iterator.reset();
        return m_iterator;
    }

    // ----------------------------------------------------------------
    @Override
    public int size() {
        return m_nElements;
    }

    // ----------------------------------------------------------------
    @Override
    public boolean isEmpty() {
        return 0 == m_nElements;
    }

    // ----------------------------------------------------------------
    @Override
    public boolean contains(Object o) {
        for (int nIndex = 0; nIndex < m_nElements; nIndex++) {
            if ((null == o && null == m_elements[nIndex]) || (null != o && o.equals(m_elements[nIndex]))) {
                return true;
            }
        }
        return false;
    }

    // ----------------------------------------------------------------
    @Override
    public boolean add(T t) {
        for (int nIndex = 0; nIndex < m_nElements; nIndex++) {
            if ((null == t && null == m_elements[nIndex]) || (null != t && t.equals(m_elements[nIndex]))) {
                return false;
            }
        }
        if (m_elements.length == m_nElements) {
            // noinspection unchecked
            T[] elements = (T[]) new Object[m_elements.length * 2];
            System.arraycopy(m_elements, 0, elements, 0, m_nElements);
            m_elements = elements;
        }
        m_elements[m_nElements++] = t;
        return true;
    }

    // ----------------------------------------------------------------
    @Override
    public boolean remove(Object o) {
        for (int nIndex = 0; nIndex < m_nElements; nIndex++) {
            if ((null == o && null == m_elements[nIndex]) || (null != o && o.equals(m_elements[nIndex]))) {
                m_elements[nIndex] = m_elements[--m_nElements];
                m_elements[m_nElements] = null;
                return true;
            }
        }
        return false;
    }

    // ----------------------------------------------------------------
    @Override
    public void clear() {
        Arrays.fill(m_elements, 0, m_nElements, null);
        m_nElements = 0;
    }

    // ----------------------------------------------------------------
    @Override
    public LowGarbageArraySet<T> clone() {
        return new LowGarbageArraySet<T>(m_elements.clone(), m_nElements);
    }

    // ----------------------------------------------------------------
    private class MyIterator implements Iterator<T> {

        private int m_nNextIndex;
        private boolean m_bCanRemove;

        // ------------------------------------------------------------
        public void reset() {
            m_nNextIndex = 0;
            m_bCanRemove = false;
        }

        // ------------------------------------------------------------
        @Override
        public boolean hasNext() {
            return m_nNextIndex < m_nElements;
        }

        // ------------------------------------------------------------
        @Override
        public T next() {
            if (m_nNextIndex == m_nElements) {
                throw new NoSuchElementException();
            }
            m_bCanRemove = true;
            return m_elements[m_nNextIndex++];
        }

        // ------------------------------------------------------------
        @Override
        public void remove() {
            if (!m_bCanRemove) {
                throw new IllegalStateException();
            } else {
                m_bCanRemove = false;
            }
            m_elements[--m_nNextIndex] = m_elements[--m_nElements];
            m_elements[m_nElements] = null;
        }
    }
}
