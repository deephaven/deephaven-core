package io.deephaven.web.shared.ide.lsp;

import com.google.gwt.core.client.JavaScriptObject;
import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;

import static io.deephaven.web.shared.fu.JsArrays.setArray;

@JsType(namespace = "dh.lsp")
public class CompletionItem implements Serializable {
    private int start;
    private int length;
    public String label;
    public int kind;
    public String detail;
    public String documentation;
    public boolean deprecated;
    public boolean preselect;
    public TextEdit textEdit;
    public String sortText;
    public String filterText;
    public int insertTextFormat;
    private TextEdit[] additionalTextEdits;
    private String[] commitCharacters;

    public CompletionItem() {
        start = length = 0;
    }

    /**
     * This constructor matches CompletionFragment semantics; it is here to ease the transition to
     * the LSP model.
     */
    @JsIgnore
    public CompletionItem(int start, int length, String completion, String displayed,
        String source) {
        this(start, length, completion, displayed,
            DocumentRange.rangeFromSource(source, start, length));
    }

    @JsIgnore
    public CompletionItem(int start, int length, String completion, String displayed,
        DocumentRange range) {
        this();
        textEdit = new TextEdit();
        textEdit.text = completion;
        textEdit.range = range;
        insertTextFormat = 2; // snippet format is insertTextFormat=2 in lsp, and insertTextRules=4.
                              // See MonacoCompletionProvider.jsx.
        label = displayed == null ? completion : displayed;
        this.start = start;
        this.length = length;
    }

    @JsProperty(name = "commitCharacters")
    public Object commitCharacters_() {
        return getCommitCharacters();
    }

    @JsIgnore
    public String[] getCommitCharacters() {
        return commitCharacters == null ? new String[0] : commitCharacters;
    }

    @JsProperty
    public void setCommitCharacters(Object args) {
        setArray(args, a -> this.commitCharacters = a);
    }

    @JsProperty(name = "additionalTextEdits")
    public Object additionalTextEdits_() {
        if (additionalTextEdits != null) {
            return Js
                .cast(Js.<JsArray<TextDocumentContentChangeEvent>>uncheckedCast(additionalTextEdits)
                    .slice());
        } else {
            return null;
        }
    }

    @JsIgnore
    public Object getAdditionalTextEdits() {
        return additionalTextEdits;
    }

    @JsIgnore
    public CompletionItem sortText(String sortText) {
        this.sortText = sortText;
        return this;
    }

    public void addAdditionalTextEdits(TextEdit... edit) {
        if (this.additionalTextEdits == null) {
            setAdditionalTextEdits(edit);
        } else {
            int existing = additionalTextEdits.length;
            additionalTextEdits = Arrays.copyOf(this.additionalTextEdits, existing + edit.length);
            System.arraycopy(edit, 0, additionalTextEdits, existing, edit.length);
        }
    }

    @JsProperty
    public void setAdditionalTextEdits(Object args) {
        if (args == null || args instanceof TextEdit[]) {
            additionalTextEdits = (TextEdit[]) args;
        } else if (args instanceof JavaScriptObject) {
            // this is actually javascript. We can do terrible things here and it's ok
            final int length = Array.getLength(args);
            final TextEdit[] typed = new TextEdit[length];
            System.arraycopy(args, 0, typed, 0, length);
            this.additionalTextEdits = typed;
        } else {
            throw new IllegalArgumentException("Not a TextEdit[] or js []" + args);
        }
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
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(additionalTextEdits, that.additionalTextEdits);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = start;
        result = 31 * result + length;
        result = 31 * result + textEdit.hashCode();
        result = 31 * result + Arrays.hashCode(additionalTextEdits);
        return result;
    }
}
