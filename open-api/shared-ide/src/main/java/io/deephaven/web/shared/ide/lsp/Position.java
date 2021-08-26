package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class Position implements Serializable {
    public int line;
    public int character;

    public Position() {}

    @JsIgnore
    public Position(Position source) {
        this();
        this.line = source.line;
        this.character = source.character;
    }

    @JsIgnore
    public Position(int line, int character) {
        this();
        this.line = line;
        this.character = character;
    }

    @JsIgnore
    public Position(JsPropertyMap<Object> source) {
        this();

        if (source.has("line")) {
            line = source.getAny("line").asInt();
        }

        if (source.has("character")) {
            character = source.getAny("character").asInt();
        }
    }

    @Override
    @JsIgnore
    public String toString() {
        return "Position{" +
                "line=" + line +
                ", character=" + character +
                '}';
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final Position position = (Position) o;

        if (line != position.line)
            return false;
        return character == position.character;
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = line;
        result = 31 * result + character;
        return result;
    }

    public boolean lessThan(Position start) {
        return line == start.line ? character < start.character : line < start.line;
    }

    public boolean lessOrEqual(Position start) {
        return line == start.line ? character <= start.character : line < start.line;
    }

    public boolean greaterThan(Position end) {
        return line == end.line ? character > end.character : line > end.line;
    }

    public boolean greaterOrEqual(Position end) {
        return line == end.line ? character >= end.character : line > end.line;
    }

    @JsIgnore
    public int extend(Position requested) {
        if (line != requested.line) {
            throw new IllegalArgumentException(
                    "Can only extend on same-line; " + this + " and " + requested + " are not on same line");
        }
        int delta = requested.character - character;
        character = requested.character;
        return delta;
    }

    @JsIgnore
    public Position plus(int line, int character) {
        return new Position(this.line + line, this.character + character);
    }

    @JsIgnore
    public Position minus(int line, int character) {
        return new Position(this.line - line, this.character - character);
    }

    public Position copy() {
        return new Position(line, character);
    }
}
