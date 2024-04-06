//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;
import java.util.Objects;

@JsType(namespace = "dh.lsp")
public class MarkupContent implements Serializable {
    public String kind;
    public String value;

    @Override
    @JsIgnore
    public String toString() {
        return "MarkupContent{" +
                "kind=" + kind +
                ", value=" + value +
                '}';
    }
}
