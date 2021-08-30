package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class VersionedTextDocumentIdentifier extends TextDocumentIdentifier
    implements Serializable {
    public int version;

    public VersionedTextDocumentIdentifier() {
        super();
    }

    @JsIgnore
    public VersionedTextDocumentIdentifier(String uri, int version) {
        this();

        this.uri = uri;
        this.version = version;
    }

    @JsIgnore
    public VersionedTextDocumentIdentifier(JsPropertyMap<Object> source) {
        this();

        if (source.has("uri")) {
            uri = source.getAny("uri").asString();
        }

        if (source.has("version")) {
            version = source.getAny("version").asInt();
        }
    }
}
