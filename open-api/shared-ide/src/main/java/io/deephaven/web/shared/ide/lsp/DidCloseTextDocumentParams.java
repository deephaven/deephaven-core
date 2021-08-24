package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class DidCloseTextDocumentParams implements Serializable {
    public TextDocumentIdentifier textDocument;


    public DidCloseTextDocumentParams() {}

    @JsIgnore
    public DidCloseTextDocumentParams(JsPropertyMap<Object> source) {
        this();

        if (source.has("textDocument")) {
            textDocument =
                new TextDocumentIdentifier(source.getAny("textDocument").asPropertyMap());
        }
    }
}
