package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class TextDocumentPositionParams implements Serializable {
    public TextDocumentIdentifier textDocument;
    public Position position;

    public TextDocumentPositionParams() {}

    @JsIgnore
    TextDocumentPositionParams(JsPropertyMap<Object> source) {
        this();

        this.updateFromJsPropertyMap(source);
    }

    protected void updateFromJsPropertyMap(JsPropertyMap<Object> source) {
        if (source.has("textDocument")) {
            textDocument =
                new TextDocumentIdentifier(source.getAny("textDocument").asPropertyMap());
        }

        if (source.has("position")) {
            position = new Position(source.getAny("position").asPropertyMap());
        }
    }
}
