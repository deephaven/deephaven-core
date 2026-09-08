//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

/**
 * Hover information for a position in a document.
 *
 * This is a JS-exposed model type ({@code dh.lsp.Hover}) that closely follows the Language Server Protocol hover
 * response shape.
 */
@JsType(namespace = "dh.lsp")
public class Hover implements Serializable {
    /**
     * The hover contents to display.
     */
    public MarkupContent contents;

    /**
     * The range within the document that this hover applies to.
     */
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
