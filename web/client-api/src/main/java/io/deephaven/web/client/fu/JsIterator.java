/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.fu;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Iterator;

/**
 * This is part of EcmaScript 2015, documented here for completeness. It supports a single method, `next()`, which
 * returns an object with a `boolean` named `done` (true if there are no more items to return; false otherwise), and
 * optionally some `T` instance, `value`, if there was at least one remaining item.
 * 
 * @param <T>
 */
@TsName(namespace = JsPackage.GLOBAL, name = "Iterator")
@TsInterface
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


    @TsName(name = "IIterableResult", namespace = JsPackage.GLOBAL)
    @TsInterface
    public interface JsIIterableResult<T> {
        @JsIgnore
        static <T> JsIIterableResult<T> create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        T getValue();

        @JsProperty
        void setValue(T value);

        @JsProperty
        boolean isDone();

        @JsProperty
        void setDone(boolean done);
    }
}
