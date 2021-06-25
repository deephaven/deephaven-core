package io.deephaven.lang.generated;

import io.deephaven.lang.parse.LspTools;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;
import io.deephaven.proto.backplane.script.grpc.DocumentRangeOrBuilder;
import io.deephaven.proto.backplane.script.grpc.Position;
import io.deephaven.proto.backplane.script.grpc.PositionOrBuilder;
import io.deephaven.web.shared.fu.LinkedIterable;
import io.deephaven.web.shared.fu.MappedIterable;

import java.util.function.UnaryOperator;

/**
 * A supertype for our generated token class.
 *
 * This is cleaner than us maintaining a complete copy of Token
 */
public class BaseToken implements Comparable<BaseToken> {

    private class ReversibleTokenIterable extends LinkedIterable<Token> {
        private final Token start;
        private final Token end;

        public ReversibleTokenIterable(
            Token start,
            Token end,
            UnaryOperator<Token> direction
        ) {
            super(start, end, true, end != null, direction);
            this.start = start;
            this.end = end;
        }

        // We override reverse() so we can avoid paying O(n) for reverse() when start and end are both known.
        @Override
        public MappedIterable<Token> reverse() {
            // We use reverse() on these iterables, and, when end!=null, we can do it w/out an O(n) traversal.
            if (end == null) {
                // If no end was supplied, we don't know where to reverse from.  O(n) for you
                // (we iterate into a list and then read it back to you, backwards);
                // we don't maintain an updated tail pointer, though we reasonably could pass around a Reference
                // (supply it when doing the final "document's done parsing" backlinking).
                return super.reverse();
            } else {
                // go backwards, including both start and end.
                if (end.prev == null) {
                    // in case this is called before the document is finalized,
                    // we can fill-in null prev links on demand.  We don't do this in javacc mechanics
                    // due to a) very high cost to change / maintain, and b) we change the tokens around when parsing,
                    // and do a full, correct back-linking at the end of parsing.
                    start.addBackLinks(end);
                }
                return new ReversibleTokenIterable(end, start, Token::prev);
            }
        }
    }

    // These are the absolute indices over the whole document,
    // rather than line/col pairs (which we may eventually adopt instead, depending on clients)
    public int startIndex = 0;
    public int endIndex = 0;
    public int tokenBegin = 0;
    Token prev;

    public boolean detached;

    public Token addBackLinks() {
        return addBackLinks(null);
    }

    public Token addBackLinks(Token until) {
        Token t = self(), n;
        while ((n = t.next) != until) {
            n.prev = t;
            t = n;
        }
        if (until != null) {
            until.prev = t;
        }
        return self();
    }

    public Token prev() {
        return prev;
    }

    // Create a method here to handle peek-state; i.e. to cache a peek-ahead for a given match type,
    // so subsequent LOOKAHEADs do not re-run the whole char scan many times.

    @Override
    public int compareTo(BaseToken o) {
        if (o == this) {
            return 0;
        }
        int myStart = getStartIndex();
        int yourStart = o.getStartIndex();
        int myEnd = getEndIndex();
        int yourEnd = o.getEndIndex();

        if (myStart == yourStart) {
            if (myEnd == yourEnd) {
                // yikers...  we shouldn't be doing this.
                assert self().image.isEmpty() && o.self().image.isEmpty() : "Different non-EOF tokens have the same indices.";
                return System.identityHashCode(this) - System.identityHashCode(o);
            }
            return myEnd - yourEnd;
        }
        return myStart - yourStart;
    }

    public boolean containsIndex(int desired) {
        return desired >= (getStartIndex() - specialTokenLength()) && desired <= getEndIndex();
    }

    public int getEndIndex() {
        Token self = ((Token)this);
//        return self.kind == ChunkerConstants.EOF ? endIndex + specialTokenLength() : endIndex;
        return endIndex;
    }

    public int getStartIndex() {
        Token self = ((Token)this);
        return self.startIndex - specialTokenLength();
    }

    public int specialTokenLength() {
        // nasty cheat, so we don't have to also copy in the Node class itself.
        Token special = ((Token)this).specialToken;
        int size = 0;
        while (special != null) {
            size += special.image.length();
            special = special.specialToken;
        }
        return size;
    }

    public void copyToken(Token token) {
        Token self = (Token) this;
        self.startIndex = token.startIndex;
        self.endIndex = token.endIndex;

        self.endColumn = token.endColumn;
        self.endLine = token.endLine;
        self.beginColumn = token.beginColumn;
        self.beginLine = token.beginLine;
        self.tokenBegin = token.tokenBegin;

        if (self.kind == 0) {
            self.kind = token.kind;
        }
        if (self.image == null) {
            self.image = token.image;
        }
        if (self.next == null) {
            self.next = token.next;
        }
        if (self.specialToken == null) {
            self.specialToken = token.specialToken;
        }
    }

    public Token next(){
        return self().next;
    }

    public Token special(){
        return self().specialToken;
    }

    private Token self() {
        return (Token)this;
    }

    public Token getNextNonWhitespace() {
        Token search = next();
        while (search != null && search.image.trim().isEmpty()) {
            search = search.next;
        }
        return search;
    }

    public String dump() {
        final Token t = self();
        return "Token{" +
            "kind=" + t.kind +
            ", beginLine=" + t.beginLine +
            ", beginColumn=" + t.beginColumn +
            ", endLine=" + t.endLine +
            ", endColumn=" + t.endColumn +
            ", image='" + t.image + '\'' +
            ", startIndex=" + startIndex +
            ", endIndex=" + endIndex +
            "} ";
    }

    /**
     * Converts our 1-indexed javacc start token to a 0-indexed LSP Position
     */
    public Position.Builder positionStart() {
        return Position.newBuilder().setLine(
            self().beginLine - 1).setCharacter(
            self().beginColumn - 1);
    }

    /**
     * Converts our 1-indexed javacc end token to a 0-indexed LSP Position.
     *
     * Does not consider javacc's weird EOF handling specially;
     * the final end position will land outside the document's actual range,
     * which is perfectly acceptable to Monaco when telling it what chars to replace
     * (it's actually required when the cursor is at the end of the document; a common occurrence).
     */
    public Position.Builder positionEnd() {
        return positionEnd(false);
    }

    /**
     *
     * Converts our 1-indexed javacc end token to a 0-indexed LSP Position.
     *
     * @param considerEof Whether or not to normalize javacc's weird EOF token handling.
     *                    Normally false, which allows the position to exceed the document's size.
     *                    This is acceptable to Monaco.
     *
     *                    EOF is only considered during internal operations when we are looking up tokens
     *                    from cursor positions (and can likely be refactored / deleted when we stop using
     *                    absolute cursor positions).
     * @return A 0-indexed Position describing the end of this token.
     */
    public Position.Builder positionEnd(boolean considerEof) {
        final Token tok = self();
        final Position.Builder pos = Position.newBuilder().setLine(
            tok.endLine - 1).setCharacter(
            tok.endColumn
        );
        // javacc line/columns are weird.
        // both a zero length EOF token and a token with image.length == 1
        // will both have the same line/columns; the start and end indexes are inclusive.
        // we normally want to use them as an exclusive end (so start + length == end).
        if (considerEof && tok.image.length() == 0) {
            pos.setCharacter(pos.getCharacter()-1);
        }
        return pos;
    }

    /**
     *
     * Returns an iterable which runs backwards from this token.
     * If you specify an end token, the iterator will include that token and stop.
     *
     * If an end token is not specified, the iterable will run backwards until the start token.
     *
     * @param end optional end token, if specified, will be the last token returned by iterator.
     */
    public MappedIterable<Token> toReverse(Token end) {
        if (end == null) {
            // This won't really be any better for reversibility, at least, not without adding
            // a `Reference<Token> tail` to this class.
            return new ReversibleTokenIterable(self(), null, BaseToken::prev);
        }
        // This iterable takes no work to create; it also handles filling in backlinks if they are needed.
        return end.to(self()).reverse();
    }

    public MappedIterable<Token> to(Token end) {
        final Token start = self();
        return new ReversibleTokenIterable(start, end, Token::next);
    }

    public boolean containsPosition(PositionOrBuilder pos) {
        return LspTools.lessOrEqual(positionStart(), pos) && LspTools.greaterOrEqual(positionEnd(), pos);
    }
    public boolean contains(DocumentRangeOrBuilder replaceRange) {
        final Position.Builder myStart = positionStart();
        final Position.Builder myEnd = positionEnd();
        return LspTools.isInside(replaceRange, myStart, myEnd);
    }
}
