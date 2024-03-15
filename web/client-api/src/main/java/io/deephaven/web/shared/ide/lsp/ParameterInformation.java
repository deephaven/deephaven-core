//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class ParameterInformation implements Serializable {
    public String label;
    public MarkupContent documentation;

    @Override
    @JsIgnore
    public String toString() {
        return "ParameterInformation{" +
                "label=" + label +
                ", documentation='" + documentation + '\'' +
                '}';
    }
}
