package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp", name = "Range")
public class DocumentRange implements Serializable {
    public Position start;
    public Position end;

    public DocumentRange() {}

    @JsIgnore
    public DocumentRange(DocumentRange fixed) {
        this();
        start = new Position(fixed.start);
        end = new Position(fixed.end);
    }

    @JsIgnore
    public DocumentRange(Position start, Position end) {
        this();
        this.start = start;
        this.end = end;
    }

    @JsIgnore
    public DocumentRange(JsPropertyMap<Object> source) {
        this();

        if (source.has("start")) {
            start = new Position(source.getAny("start").asPropertyMap());
        }

        if (source.has("end")) {
            end = new Position(source.getAny("end").asPropertyMap());
        }
    }

    @JsIgnore
    public static DocumentRange rangeFromSource(String source, int start, int length) {
        final DocumentRange range = new DocumentRange();
        range.start = getPositionFromOffset(source, start);
        range.end = getPositionFromOffset(
            source,
            start + length);
        return range;
    }

    // some document change helpers
    @JsIgnore
    public static int getOffsetFromPosition(String document, Position position) {
        int offset = 0;
        int line = 0;
        while (line < position.line) {
            offset = document.indexOf("\n", offset);
            if (offset < 0) {
                // This position in the document is more lines than we have
                return -1;
            }
            offset += 1;
            line += 1;
        }

        offset += position.character;

        return offset;
    }

    @JsIgnore
    public static Position getPositionFromOffset(String document, int offset) {
        Position position = new Position();
        int pos = 0;
        while (offset > 0) {
            int nextLineOffset = document.indexOf("\n", pos);
            if (nextLineOffset >= 0) {
                position.line += 1;
                position.character = 0;
                offset -= (1 + nextLineOffset - pos);
            } else {
                position.character = offset;
                break;
            }

            pos = nextLineOffset + 1;
        }
        return position;
    }

    @Override
    @JsIgnore
    public String toString() {
        return "Range{" +
            "start=" + start +
            ", end=" + end +
            '}';
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final DocumentRange range = (DocumentRange) o;

        if (!start.equals(range.start))
            return false;
        return end.equals(range.end);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = start.hashCode();
        result = 31 * result + end.hashCode();
        return result;
    }

    @JsIgnore
    public void decrementColumns() {
        start.character--;
        end.character--;
    }

    public boolean isInside(Position innerStart, Position innerEnd) {
        return innerStart.line >= start.line && innerStart.character >= start.character
            && innerEnd.line <= end.line && innerEnd.character <= end.character;
    }
}
