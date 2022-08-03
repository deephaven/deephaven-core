/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.fu;

import elemental2.core.JsIIterableResult;
import jsinterop.annotations.JsMethod;

import java.util.Iterator;

public class JsIterator<T> {

    public JsIterator(Iterator<T> wrapped) {
        this.wrapped = wrapped;
    }

    private final Iterator<T> wrapped;

    @JsMethod
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

        return result;
    }

}
