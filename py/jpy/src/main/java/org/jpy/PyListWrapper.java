/*
 * Copyright 2015 Brockmann Consult GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * This file was modified by Deephaven Data Labs.
 *
 */
package org.jpy;

import java.util.*;

/**
 * A simple wrapper around a Python List object that implements a java List of PyObjects.
 */
public class PyListWrapper implements List<PyObject> {
    // todo: https://docs.python.org/3/c-api/list.html vs https://docs.python.org/3/c-api/sequence.html
    private PyObject pyObject;

    PyListWrapper(PyObject pyObject) {
        this.pyObject = pyObject;
    }

    @Override
    public int size() {
        try (final PyObject len = this.pyObject.callMethod("__len__")) {
            return len.getIntValue();
        }
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        for (PyObject obj : this) {
            if (obj.equals(o)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Iterator<PyObject> iterator() {
        return new Iterator<PyObject>() {
            int ii = 0;
            int size = size();

            @Override
            public boolean hasNext() {
                return ii < size;
            }

            @Override
            public PyObject next() {
                return get(ii++);
            }
        };
    }

    @Override
    public PyObject[] toArray() {
        int size = size();

        PyObject [] result = new PyObject[size];
        for (int ii = 0; ii < size; ++ii) {
            result[ii] = get(ii);
        }

        return result;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        int size = size();

        if (a.length < size) {
            a = Arrays.copyOf(a, size);
        }
        for (int ii = 0; ii < size; ++ii) {
            //noinspection unchecked
            a[ii] = (T)get(ii);
        }
        if (a.length > size) {
            a[size] = null;
        }

        return a;
    }

    @Override
    public boolean add(PyObject pyObject) {
        //noinspection EmptyTryBlock
        try (final PyObject obj = pyObject.callMethod("append", pyObject)) {

        }
        return true;
    }

    @Override
    public boolean remove(Object o) {
        try {
            //noinspection EmptyTryBlock
            try (final PyObject obj = pyObject.callMethod("remove", o)) {

            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return c.stream().allMatch(this::contains);
    }

    @Override
    public boolean addAll(Collection<? extends PyObject> c) {
        boolean result = false;
        for (PyObject po : c) {
            result |= add(po);
        }
        return result;
    }

    @Override
    public boolean addAll(int index, Collection<? extends PyObject> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        //noinspection EmptyTryBlock
        try (final PyObject obj = pyObject.callMethod("clear")) {

        }
    }

    @Override
    public PyObject get(int index) {
        // todo https://docs.python.org/3/c-api/list.html#c.PyList_GetItem
        return pyObject.callMethod("__getitem__", index);
    }

    @Override
    public PyObject set(int index, PyObject element) {
        final PyObject existing = get(index);
        setItem(index, element);
        return existing;
    }

    public void setItem(int index, PyObject element) {
        //noinspection EmptyTryBlock
        try (final PyObject obj = pyObject.callMethod("__setitem__", index, element)) {

        }
    }

    @Override
    public void add(int index, PyObject element) {
        //noinspection EmptyTryBlock
        try (final PyObject obj = pyObject.callMethod("insert", index, element)) {

        }
    }

    @Override
    public PyObject remove(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int indexOf(Object o) {
        int size = size();

        for (int ii = 0; ii < size; ++ii) {
            PyObject pyObject = get(ii);
            if (pyObject == null ? o == null : pyObject.equals(o)) {
                return ii;
            }
        }

        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        int size = size();

        for (int ii = size - 1; ii >= 0; --ii) {
            PyObject pyObject = get(ii);
            if (pyObject == null ? o == null : pyObject.equals(o)) {
                return ii;
            }
        }

        return -1;
    }

    @Override
    public ListIterator<PyObject> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<PyObject> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<PyObject> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

    /**
     * Return a summarized preview of this list.
     *
     * @param prefixLength The maximum prefix of the array to convert
     * @return The String representation of array
     */
    public String toString(final int prefixLength) {
        if (isEmpty()) {
            return "[]";
        }
        final StringBuilder builder = new StringBuilder("[");
        final int displaySize = Math.min(size(), prefixLength);
        builder.append(get(0).str());
        for (int ei = 1; ei < displaySize; ++ei) {
            builder.append(", ").append(get(ei).str());
        }
        if (displaySize == size()) {
            builder.append(']');
        } else {
            builder.append(", ...]");
        }
        return builder.toString();
    }
}
