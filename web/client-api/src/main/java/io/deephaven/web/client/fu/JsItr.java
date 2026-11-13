//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.fu;

import elemental2.core.JsArray;
import elemental2.core.JsIIterableResult;
import elemental2.core.JsIterator;
import elemental2.core.JsIteratorIterable;
import elemental2.core.JsMap;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.MappedIterable;
import jsinterop.annotations.JsMethod;
import jsinterop.base.Js;

import java.util.Iterator;
import java.util.List;

/**
 * Some shorthand utils for getting java iterables out of javascript generated iterable types and vice versa.
 */
public class JsItr {
    private static final Iterator NO_REUSE = new Iterator() {
        @Override
        public boolean hasNext() {
            throw new UnsupportedOperationException("Iterator not reusable");
        }

        @Override
        public Object next() {
            throw new UnsupportedOperationException("Iterator not reusable");
        }
    };

    public static <T> MappedIterable<T> iterate(JsIteratorIterable<T, Object, Object> values) {
        final Iterator<T> itr = new Iterator<T>() {

            public JsIIterableResult<T> next;

            @Override
            public boolean hasNext() {
                if (next == null) {
                    next = values.next();
                }
                return !next.isDone();
            }

            @Override
            public T next() {
                try {
                    return next.getValue();
                } finally {
                    next = null;
                }
            }
        };
        Iterator[] itrs = new Iterator[] {itr};
        return () -> {
            final Iterator toReturn = itrs[0];
            itrs[0] = NO_REUSE;
            return toReturn;
        };
    }

    /**
     * js forEach signature is a bit weird, so we'll adapt it to something saner here
     */
    public static <K, V> void forEach(JsMap<K, V> map, JsBiConsumer<K, V> callback) {
        map.forEach((v, k) -> {
            callback.apply(k, v);
            return null;
        });
    }

    public static <T> JsArray<T> slice(List<T> currentSort) {
        return Js.uncheckedCast(Js.<JsArray<T>>uncheckedCast(currentSort.toArray()).slice());
    }

    /**
     * Rewrites a Java Iterator into a JS iterator.
     */
    public static <T> JsIterator<T, Void, Void> jsIterator(Iterator<T> iterator) {
        return Js.uncheckedCast(new IteratorWrapper<>(iterator));
    }

    private static class IteratorWrapper<T> {
        private final Iterator<T> wrapped;

        private IteratorWrapper(Iterator<T> wrapped) {
            this.wrapped = wrapped;
        }

        public boolean hasNext() {
            return wrapped.hasNext();
        }

        @JsMethod
        public JsIIterableResult<T> next() {
            JsIIterableResult<T> result = JsIIterableResult.create();
            if (hasNext()) {
                result.setValue(wrapped.next());
            }
            result.setDone(hasNext());

            return Js.uncheckedCast(result);
        }
    }
}
