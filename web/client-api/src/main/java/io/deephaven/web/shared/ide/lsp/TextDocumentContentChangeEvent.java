//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

/**
 * A document content change.
 *
 * This is a JS-exposed model type ({@code dh.lsp.TextDocumentContentChangeEvent}) that closely follows the Language
 * Server Protocol text document content change event shape.
 */
@JsType(namespace = "dh.lsp")
public class TextDocumentContentChangeEvent implements Serializable {
    /**
     * The range of the document that the change applies to.
     */
    public DocumentRange range;

    /**
     * The length of the replaced range.
     */
    public int rangeLength;

    /**
     * The replacement text.
     */
    public String text;

    public TextDocumentContentChangeEvent() {}

    @JsIgnore
    public TextDocumentContentChangeEvent(DocumentRange range, int rangeLength, String text) {
        this();

        this.range = range;
        this.rangeLength = rangeLength;
        this.text = text;
    }

    @JsIgnore
    public TextDocumentContentChangeEvent(JsPropertyMap<Object> source) {
        this();

        if (source.has("range")) {
            range = new DocumentRange(source.getAsAny("range").asPropertyMap());
        }

        if (source.has("rangeLength")) {
            rangeLength = source.getAsAny("rangeLength").asInt();
        }

        if (source.has("text")) {
            text = source.getAsAny("text").asString();
        }
    }

    @Override
    @JsIgnore
    public String toString() {
        return "TextDocumentContentChangeEvent{" +
                "range=" + range +
                ", rangeLength=" + rangeLength +
                ", text='" + text + '\'' +
                '}';
    }
}
