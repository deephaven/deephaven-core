//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

/**
 * A text edit to apply to a document.
 *
 * This is a JS-exposed model type ({@code dh.lsp.TextEdit}) that closely follows the Language Server Protocol text edit
 * shape.
 */
@JsType(namespace = "dh.lsp")
public class TextEdit implements Serializable {
    /**
     * The range of the document to replace.
     */
    public DocumentRange range;

    /**
     * The replacement text.
     */
    public String text;

    @Override
    @JsIgnore
    public String toString() {
        return "TextEdit{" +
                "range=" + range +
                ", text='" + text + '\'' +
                '}';
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TextEdit textEdit = (TextEdit) o;

        if (!range.equals(textEdit.range))
            return false;
        return text.equals(textEdit.text);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = range.hashCode();
        result = 31 * result + text.hashCode();
        return result;
    }
}
