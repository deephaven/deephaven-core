package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.*;
import jsinterop.base.Any;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

/**
 * Represents a diagnostic message sent to the client, to mark info/warn/error in the source.
 * <p>
 * Original definition in current lsp documentation,
 * https://microsoft.github.io/language-server-protocol/specification (text search Diagnostic;
 * couldn't find good intra-document name='d links)
 * <p>
 * All field documentation here is copy-pasted from original source.
 */
@JsType(namespace = "dh.lsp")
public class Diagnostic implements Serializable {
    /**
     * The range at which the message applies.
     */
    private DocumentRange range;

    /**
     * The diagnostic's severity. Can be omitted. If omitted it is up to the client to interpret
     * diagnostics as error, warning, info or hint.
     */
    private Integer severity;

    /**
     * The diagnostic's code, which might appear in the user interface.
     */
    private Integer code;

    /**
     * A human-readable string describing the source of this diagnostic, e.g. 'typescript' or 'super
     * lint'.
     */
    private String source;

    /**
     * The diagnostic's message.
     */
    private String message;

    /**
     * An array of related diagnostic information, e.g. when symbol-names within a scope collide all
     * definitions can be marked via this property.
     */
    private DiagnosticRelatedInformation[] relatedInformation;

    @JsIgnore
    public Diagnostic() {
        this(null);
    }

    @JsConstructor
    public Diagnostic(@JsOptional JsPropertyMap<Object> source) {
        if (source == null) {
            return;
        }
        if (source.has("range")) {
            range = new DocumentRange(source.getAny("range").asPropertyMap());
        }
        if (source.has("severity")) {
            severity = source.getAny("severity").asInt();
        }
        if (source.has("code")) {
            code = source.getAny("code").asInt();
        }
        if (source.has("source")) {
            this.source = source.getAny("source").asString();
        }
        if (source.has("message")) {
            this.source = source.getAny("message").asString();
        }
        if (source.has("relatedInformation")) {
            Any[] related = source.getAny("relatedInformation").asArray();
            relatedInformation = new DiagnosticRelatedInformation[related.length];
            for (int i = 0; i < related.length; i++) {
                relatedInformation[i] =
                    new DiagnosticRelatedInformation(related[i].asPropertyMap());
            }
        }
    }

    @JsProperty
    public DocumentRange getRange() {
        return range;
    }

    @JsProperty
    public void setRange(DocumentRange range) {
        this.range = range;
    }

    @JsProperty
    public Double getSeverity() {
        return severity == null ? 0 : severity.doubleValue();
    }

    @JsProperty
    public void setSeverity(Double severity) {
        this.severity = severity == null ? null : severity.intValue();
        assert severity == null || severity == severity.intValue()
            : "Only set integer severity! (you sent " + severity + ")";
    }

    @JsProperty
    public Double getCode() {
        return code == null ? 0 : code.doubleValue();
    }

    @JsIgnore // for java
    public void code(int code) {
        this.code = code;
    }

    @JsProperty // for js
    public void setCode(Double code) {
        this.code = code == null ? null : code.intValue();
        assert code == null || code == code.intValue()
            : "Only set integer code! (you sent " + code + ")";
    }

    @JsProperty
    public String getSource() {
        return source;
    }

    @JsProperty
    public void setSource(String source) {
        this.source = source;
    }

    @JsProperty
    public String getMessage() {
        return message;
    }

    @JsProperty
    public void setMessage(String message) {
        this.message = message;
    }

    @JsProperty
    public DiagnosticRelatedInformation[] getRelatedInformation() {
        return relatedInformation;
    }

    @JsProperty
    public void setRelatedInformation(DiagnosticRelatedInformation[] relatedInformation) {
        this.relatedInformation = relatedInformation;
    }
}
