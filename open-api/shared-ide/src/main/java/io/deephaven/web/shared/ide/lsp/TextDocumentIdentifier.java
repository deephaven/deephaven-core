package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class TextDocumentIdentifier implements Serializable {
    public String uri;

    public TextDocumentIdentifier() {}

    @JsIgnore
    public TextDocumentIdentifier(String uri) {
        this();

        this.uri = uri;
    }

    @JsIgnore
    public TextDocumentIdentifier(JsPropertyMap<Object> source) {
        this();

        if (source.has("uri")) {
            uri = source.getAsAny("uri").asString();
        }
    }
}
