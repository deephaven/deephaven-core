package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class TextDocumentItem implements Serializable {
    @JsConstructor
    public TextDocumentItem() {}

    @JsIgnore
    public TextDocumentItem(JsPropertyMap<Object> source) {
        this();

        if (source.has("uri")) {
            uri = source.getAny("uri").asString();
        }

        if (source.has("languageId")) {
            languageId = source.getAny("languageId").asString();
        }

        if (source.has("version")) {
            version = source.getAny("version").asInt();
        }

        if (source.has("text")) {
            text = source.getAny("text").asString();
        }
    }

    public String uri;
    public String languageId;
    private int version;
    public String text;

    @JsProperty
    public int getVersion() {
        return version;
    }

    @JsProperty
    public void setVersion(int version) {
        this.version = version;
    }
}
