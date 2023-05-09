/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.fu;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.Iterator;

@JsType(namespace = JsPackage.GLOBAL, name = "Iterator")
@TsInterface
public class JsIterator<T> {

    @JsIgnore
    public JsIterator(Iterator<T> wrapped) {
        this.wrapped = wrapped;
    }

    private final Iterator<T> wrapped;

    public boolean hasNext() {
        return wrapped.hasNext();
    }

    public IIterableResult<T> next() {
        JsIIterableResult<T> result = JsIIterableResult.create();
        if (hasNext()) {
            result.setValue(wrapped.next());
        }
        result.setDone(hasNext());

        return Js.uncheckedCast(result);
    }

    @JsType(name = "IIterableResult", namespace = JsPackage.GLOBAL)
    @TsInterface
    public interface IIterableResult<T> {
        @JsProperty
        T getValue();

        @JsProperty
        void setValue(T value);

        @JsProperty
        boolean isDone();

        @JsProperty
        void setDone(boolean done);
    }

    /**
     * Copied from elemental2, but has a concrete subclass to permit us to export it back to TS.
     */
    @JsType(name = "IIterableResult", namespace = JsPackage.GLOBAL, isNative = true)
    @TsInterface
    public interface JsIIterableResult<T> {
        @JsOverlay
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
