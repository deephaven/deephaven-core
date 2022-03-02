package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class DidOpenTextDocumentParams implements Serializable {
    @JsConstructor
    public DidOpenTextDocumentParams() {}

    @JsIgnore
    public DidOpenTextDocumentParams(JsPropertyMap<Object> source) {
        this();

        if (source.has("textDocument")) {
            textDocument = new TextDocumentItem(source.getAsAny("textDocument").asPropertyMap());
        }
    }

    public TextDocumentItem textDocument;
}
