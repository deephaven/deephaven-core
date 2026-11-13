//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import elemental2.promise.Promise;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

/**
 * The result of fetching an object by its definition: a {@link JsPendingTable} when the definition is a table, so that
 * operations can be chained on it before it arrives, and otherwise a promise of the object. Both can be awaited.
 */
@TsUnion(anonymous = false, name = "PendingObject", namespace = "dh")
@JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
public interface JsPendingObject {
    @JsOverlay
    static JsPendingObject of(JsPendingTable<JsTable> table) {
        return Js.cast(table);
    }

    @JsOverlay
    static JsPendingObject of(Promise<?> promise) {
        return Js.cast(promise);
    }

    @TsUnionMember
    @JsOverlay
    default JsPendingTable<JsTable> asPendingTable() {
        return Js.cast(this);
    }

    @TsUnionMember
    @JsOverlay
    default Promise<?> asPromise() {
        return Js.cast(this);
    }
}
