//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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

/**
 * This is part of EcmaScript 2015, documented here for completeness. It supports a single method, <b>next()</b>, which
 * returns an object with a <b>boolean</b> named <b>done</b> (true if there are no more items to return; false
 * otherwise), and optionally some <b>T</b> instance, <b>value</b>, if there was at least one remaining item.
 *
 * @param <T>
 */
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
     * 
     * <p>
     * This type is used as the return value of {@link JsIterator#next()} and matches the shape of JavaScript iterator
     * results: a boolean {@code done} flag and an optional {@code value}.
     */
    @JsType(name = "IIterableResult", namespace = JsPackage.GLOBAL, isNative = true)
    @TsInterface
    public interface JsIIterableResult<T> {
        /**
         * Creates an empty iterator result object.
         */
        @JsOverlay
        static <T> JsIIterableResult<T> create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        /**
         * The value produced by the iterator.
         *
         * <p>
         * This property may be absent or {@code null} when {@link #isDone()} is {@code true}.
         */
        @JsProperty
        T getValue();

        /**
         * Sets {@link #getValue()}.
         */
        @JsProperty
        void setValue(T value);

        /**
         * Whether the iterator has no more values to produce.
         */
        @JsProperty
        boolean isDone();

        /**
         * Sets {@link #isDone()}.
         */
        @JsProperty
        void setDone(boolean done);
    }
}
