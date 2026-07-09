//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

/**
 * Information about a callable signature.
 *
 * This is a JS-exposed model type ({@code dh.lsp.SignatureInformation}) that closely follows the Language Server
 * Protocol signature information shape.
 */
@JsType(namespace = "dh.lsp")
public class SignatureInformation implements Serializable {
    /**
     * The label of this signature.
     */
    public String label;

    /**
     * Documentation for this signature.
     */
    public MarkupContent documentation;

    /**
     * The parameters of this signature.
     */
    public JsArray<ParameterInformation> parameters;

    /**
     * The index of the active parameter.
     */
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
