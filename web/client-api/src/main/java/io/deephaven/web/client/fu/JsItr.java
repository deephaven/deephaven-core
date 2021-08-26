package io.deephaven.web.client.fu;

import elemental2.core.JsArray;
import elemental2.core.JsIIterableResult;
import elemental2.core.JsIteratorIterable;
import elemental2.core.JsMap;
import io.deephaven.web.shared.fu.JsBiConsumer;
import io.deephaven.web.shared.fu.MappedIterable;
import jsinterop.base.Js;

import java.util.Iterator;
import java.util.List;

/**
 * Some shorthand utils for getting java iterables out of javascript generated iterable types.
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

    public static <T> MappedIterable<T> iterate(JsIteratorIterable<T> values) {
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
        map.forEach((v, k, m) -> {
            callback.apply(k, v);
            return null;
        });
    }

    public static <T> JsArray<T> slice(List<T> currentSort) {
        return Js.uncheckedCast(Js.<JsArray<T>>uncheckedCast(currentSort.toArray()).slice());
    }
}
