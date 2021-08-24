package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class TextDocumentContentChangeEvent implements Serializable {
    public DocumentRange range;
    public int rangeLength;
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
            range = new DocumentRange(source.getAny("range").asPropertyMap());
        }

        if (source.has("rangeLength")) {
            rangeLength = source.getAny("rangeLength").asInt();
        }

        if (source.has("text")) {
            text = source.getAny("text").asString();
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
