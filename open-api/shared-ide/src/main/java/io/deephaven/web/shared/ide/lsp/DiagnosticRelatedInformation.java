package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

/**
 * Used to add N location+message pairs to a single {@link Diagnostic} message.
 */
@JsType(namespace = "dh.lsp")
public class DiagnosticRelatedInformation implements Serializable {
    public Location location;
    public String message;

    public DiagnosticRelatedInformation() {}

    @JsIgnore
    public DiagnosticRelatedInformation(JsPropertyMap<Object> source) {
        this();

        if (source.has("location")) {
            location = new Location(source.getAsAny("location").asPropertyMap());
        }
        if (source.has("message")) {
            message = source.getAsAny("message").asString();
        }
    }
}
