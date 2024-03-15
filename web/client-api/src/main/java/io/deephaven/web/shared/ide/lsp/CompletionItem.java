//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;
import java.util.Objects;


@JsType(namespace = "dh.lsp")
public class CompletionItem implements Serializable {
    private int start;
    private int length;
    public String label;
    public int kind;
    public String detail;
    public MarkupContent documentation;
    public boolean deprecated;
    public boolean preselect;
    public TextEdit textEdit;
    public String sortText;
    public String filterText;
    public int insertTextFormat;
    public JsArray<TextEdit> additionalTextEdits;
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
