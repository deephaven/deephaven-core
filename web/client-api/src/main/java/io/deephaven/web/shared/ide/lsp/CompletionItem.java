//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;
import java.util.Objects;


/**
 * A completion suggestion for use by the IDE language tools.
 *
 * This is a JS-exposed model type ({@code dh.lsp.CompletionItem}) that closely follows the Language Server Protocol
 * completion item shape.
 */
@JsType(namespace = "dh.lsp")
public class CompletionItem implements Serializable {
    private int start;
    private int length;

    /**
     * The primary label to display for this completion item.
     */
    public String label;

    /**
     * The kind of completion item (for example, method, variable, keyword), represented as an integer.
     */
    public int kind;

    /**
     * Additional detail text to display alongside {@link #label}.
     */
    public String detail;

    /**
     * Documentation for this completion item.
     */
    public MarkupContent documentation;

    /**
     * Whether this completion item is deprecated.
     */
    public boolean deprecated;

    /**
     * Whether this completion item should be preselected in completion UI.
     */
    public boolean preselect;

    /**
     * The edit to apply to the document when this completion item is accepted.
     */
    public TextEdit textEdit;

    /**
     * Sort text for ordering completion items.
     */
    public String sortText;

    /**
     * Filter text used to match this completion item against a query.
     */
    public String filterText;

    /**
     * The insert text format (for example, plain text vs snippet), represented as an integer.
     */
    public int insertTextFormat;

    /**
     * Additional edits to apply when this completion item is accepted.
     */
    public JsArray<TextEdit> additionalTextEdits;

    /**
     * Characters that commit this completion item if typed.
     */
    public JsArray<String> commitCharacters;

    public CompletionItem() {
        start = length = 0;
    }

    /**
     * This constructor matches CompletionFragment semantics; it is here to ease the transition to the LSP model.
     */
    @JsIgnore
    public CompletionItem(int start, int length, String completion, String displayed, String source) {
        this(start, length, completion, displayed, DocumentRange.rangeFromSource(source, start, length));
    }

    @JsIgnore
    public CompletionItem(int start, int length, String completion, String displayed, DocumentRange range) {
        this();
        textEdit = new TextEdit();
        textEdit.text = completion;
        textEdit.range = range;
        insertTextFormat = 2; // snippet format is insertTextFormat=2 in lsp, and insertTextRules=4. See
                              // MonacoCompletionProvider.jsx.
        label = displayed == null ? completion : displayed;
        this.start = start;
        this.length = length;
    }

    @JsIgnore
    public CompletionItem sortText(String sortText) {
        this.sortText = sortText;
        return this;
    }

    /**
     * This is not used for monaco or lsp; it is only here for compatibility w/ swing autocomplete
     */
    @JsIgnore
    public int getStart() {
        return start;
    }

    @JsIgnore
    public void setStart(int start) {
        this.start = start;
    }

    /**
     * This is not used for monaco or lsp; it is only here for compatibility w/ swing autocomplete
     */
    @JsIgnore
    public int getLength() {
        return length;
    }

    @JsIgnore
    public void setLength(int length) {
        this.length = length;
    }

    @Override
    @JsIgnore
    public String toString() {
        return "CompletionItem{" +
                "start=" + start +
                ", length=" + length +
                ", textEdit=" + textEdit +
                "}\n";
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final CompletionItem that = (CompletionItem) o;

        if (start != that.start)
            return false;
        if (length != that.length)
            return false;
        if (!Objects.equals(textEdit, that.textEdit))
            return false;
        return Objects.equals(additionalTextEdits, that.additionalTextEdits);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = start;
        result = 31 * result + length;
        result = 31 * result + textEdit.hashCode();
        result = 31 * result + Objects.hashCode(additionalTextEdits);
        return result;
    }
}
