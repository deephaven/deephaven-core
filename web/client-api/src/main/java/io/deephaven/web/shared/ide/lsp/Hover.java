//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class Hover implements Serializable {
    public MarkupContent contents;
    public DocumentRange range;

    @Override
    @JsIgnore
    public String toString() {
        return "Hover{" +
                "contents=" + contents +
                ", range='" + range.toString() + '\'' +
                '}';
    }
}
