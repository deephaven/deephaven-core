//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class SignatureInformation implements Serializable {
    public String label;
    public MarkupContent documentation;
    public JsArray<ParameterInformation> parameters;
    public int activeParameter;

    @Override
    @JsIgnore
    public String toString() {
        return "SignatureInformation{" +
                "label=" + label +
                ", documentation=" + documentation +
                ", parameters=" + parameters +
                ", activeParameter=" + activeParameter +
                "}\n";
    }
}
