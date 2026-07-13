//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;

/**
 * A position within a text document.
 *
 * This is a JS-exposed model type ({@code dh.lsp.Position}) that closely follows the Language Server Protocol position
 * shape.
 */
@JsType(namespace = "dh.lsp")
public class Position implements Serializable {
    /**
     * The zero-based line offset.
     */
    public int line;

    /**
     * The zero-based character offset within the line.
     */
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
            line = source.getAsAny("line").asInt();
        }

        if (source.has("character")) {
            character = source.getAsAny("character").asInt();
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

    /**
     * Checks if this position is strictly less than {@code start} (lexicographically by line then character).
     *
     * @param start the position to compare against
     * @return {@code true} if this position is before {@code start}
     */
    public boolean lessThan(Position start) {
        return line == start.line ? character < start.character : line < start.line;
    }

    /**
     * Checks if this position is less than or equal to {@code start} (lexicographically by line then character).
     *
     * @param start the position to compare against
     * @return {@code true} if this position is before or equal to {@code start}
     */
    public boolean lessOrEqual(Position start) {
        return line == start.line ? character <= start.character : line < start.line;
    }

    /**
     * Checks if this position is strictly greater than {@code end} (lexicographically by line then character).
     *
     * @param end the position to compare against
     * @return {@code true} if this position is after {@code end}
     */
    public boolean greaterThan(Position end) {
        return line == end.line ? character > end.character : line > end.line;
    }

    /**
     * Checks if this position is greater than or equal to {@code end} (lexicographically by line then character).
     *
     * @param end the position to compare against
     * @return {@code true} if this position is after or equal to {@code end}
     */
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

    /**
     * Creates a copy of this position.
     *
     * @return a new {@link Position} with the same line and character
     */
    public Position copy() {
        return new Position(line, character);
    }
}
