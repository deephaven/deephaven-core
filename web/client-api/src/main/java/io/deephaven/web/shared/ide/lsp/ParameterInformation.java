//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

/**
 * Information about a single parameter in a callable signature.
 *
 * This is a JS-exposed model type ({@code dh.lsp.ParameterInformation}) that closely follows the Language Server
 * Protocol parameter information shape.
 */
@JsType(namespace = "dh.lsp")
public class ParameterInformation implements Serializable {
    /**
     * The label of this parameter.
     */
    public String label;

    /**
     * Documentation for this parameter.
     */
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
