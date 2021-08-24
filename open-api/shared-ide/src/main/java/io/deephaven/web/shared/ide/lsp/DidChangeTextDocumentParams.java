package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class DidChangeTextDocumentParams implements Serializable {
    public VersionedTextDocumentIdentifier textDocument;
    public TextDocumentContentChangeEvent[] contentChanges;

    public DidChangeTextDocumentParams() {}

    @JsIgnore
    public DidChangeTextDocumentParams(JsPropertyMap<Object> source) {
        this();

        if (source.has("textDocument")) {
            textDocument =
                new VersionedTextDocumentIdentifier(source.getAny("textDocument").asPropertyMap());
        }

        if (source.has("contentChanges")) {
            Any[] rawContentChanges = source.getAny("contentChanges").asArray();
            contentChanges = new TextDocumentContentChangeEvent[rawContentChanges.length];
            for (int i = 0; i < rawContentChanges.length; i++) {
                contentChanges[i] =
                    new TextDocumentContentChangeEvent(rawContentChanges[i].asPropertyMap());
            }
        }
    }
}
