//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.fu;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import jsinterop.base.Js;
import org.jetbrains.annotations.NotNull;

import java.util.stream.Collector;
import java.util.stream.Collectors;

public class JsCollectors {
    @NotNull
    public static <T> Collector<T, JsArray<T>, JsArray<T>> toFrozenJsArray() {
        return Collectors.collectingAndThen(
                Collector.of(
                        JsArray::new,
                        JsArray::push,
                        (arr1, arr2) -> arr1.concat(arr2.asArray(Js.uncheckedCast(JsArray.of())))),
                JsObject::freeze);
    }
}
