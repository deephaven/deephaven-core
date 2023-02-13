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
