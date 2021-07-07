package io.deephaven.lang.parse;

import io.deephaven.proto.backplane.script.grpc.DocumentRange;
import io.deephaven.proto.backplane.script.grpc.DocumentRangeOrBuilder;
import io.deephaven.proto.backplane.script.grpc.Position;
import io.deephaven.proto.backplane.script.grpc.PositionOrBuilder;

/**
 * LspTools:
 * <p><p>
 *     This class is where we'll dump all the static "manipulate lsp-related objects".
 * <p><p>
 *     These methods used to be instance methods on hand-maintained mutable objects,
 * <p>
 *     Now, they are static utilities operating on grpc-generated immutable objects/builders.
 * <p><p>
 */
public class LspTools {

    public static int getOffsetFromPosition(String document, Position position) {
        int offset = 0;
        int line = 0;
        while (line < position.getLine()) {
            offset = document.indexOf("\n", offset);
            if (offset < 0) {
                // This position in the document is more lines than we have
                return -1;
            }
            offset += 1;
            line += 1;
        }

        offset += position.getCharacter();

        return offset;
    }

    public static boolean lessThan(PositionOrBuilder p, PositionOrBuilder start) {
        return p.getLine() == start.getLine() ? p.getCharacter() < start.getCharacter() : p.getLine() < start.getLine();
    }

    public static boolean lessOrEqual(PositionOrBuilder p, PositionOrBuilder start) {
        return p.getLine() == start.getLine() ? p.getCharacter() <= start.getCharacter() : p.getLine() < start.getLine();
    }

    public static boolean greaterThan(PositionOrBuilder p, PositionOrBuilder end) {
        return p.getLine() == end.getLine() ? p.getCharacter() > end.getCharacter() : p.getLine() > end.getLine();
    }

    public static boolean greaterOrEqual(PositionOrBuilder p, PositionOrBuilder end) {
        return p.getLine() == end.getLine() ? p.getCharacter() >= end.getCharacter() : p.getLine() > end.getLine();
    }

    public static boolean equal(PositionOrBuilder p, PositionOrBuilder end) {
        return p.getLine() == end.getLine() && p.getCharacter() == end.getCharacter();
    }

    public static int extend(Position.Builder p, PositionOrBuilder requested) {
        if (p.getLine() != requested.getLine()) {
            throw new IllegalArgumentException("Can only extend on same-line; " + p + " and " + requested + " are not on same line");
        }
        p.setCharacter(requested.getCharacter()).build();
        return requested.getCharacter() - p.getCharacter();
    }

    public static Position plus(Position p, int line, int character) {
        return p.toBuilder().setLine(p.getLine() + line).setCharacter(p.getCharacter() + character).build();
    }

    public static Position minus(Position p, int line, int character) {
        return p.toBuilder().setLine(p.getLine() - line).setCharacter(p.getCharacter() - character).build();
    }

    public static Position copy(Position p) {
        return p.toBuilder().build();
    }


    public static boolean isInside(DocumentRangeOrBuilder range, PositionOrBuilder innerStart, PositionOrBuilder innerEnd) {
        return innerStart.getLine() >= range.getStart().getLine() && innerStart.getCharacter() >= range.getStart().getCharacter()
                && innerEnd.getLine() <= range.getEnd().getLine() && innerEnd.getCharacter() <= range.getEnd().getCharacter();
    }

    public static DocumentRange.Builder rangeFromSource(String source, int start, int length) {
        final DocumentRange.Builder range = DocumentRange.newBuilder();
        range.setStart(getPositionFromOffset(source, start));
        range.setEnd(getPositionFromOffset(
                source,
                start + length
        ));
        return range;
    }

    // some document change helpers
    public static int getOffsetFromPosition(String document, PositionOrBuilder position) {
        int offset = 0;
        int line = 0;
        while (line < position.getLine()) {
            offset = document.indexOf("\n", offset);
            if (offset < 0) {
                // This position in the document is more lines than we have
                return -1;
            }
            offset += 1;
            line += 1;
        }

        offset += position.getCharacter();

        return offset;
    }

    public static Position.Builder getPositionFromOffset(String document, int offset) {
        final Position.Builder position = Position.newBuilder();
        int pos = 0;
        while (offset > 0) {
            int nextLineOffset = document.indexOf("\n", pos);
            if (nextLineOffset >= 0) {
                position.setLine(position.getLine() + 1);
                position.setCharacter(0);
                offset -= (1+nextLineOffset - pos);
            } else {
                position.setCharacter(offset);
                break;
            }

            pos = nextLineOffset+1;
        }
        return position;
    }
}
