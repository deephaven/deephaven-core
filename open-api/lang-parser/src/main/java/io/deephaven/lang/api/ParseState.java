package io.deephaven.lang.api;

import io.deephaven.lang.generated.Node;

/**
 * Represents the state of a "currently being parsed node".
 *
 * For now, we're going to use this to have our own hierarchy of ast nodes outside of the generated jjt state machine,
 * and in the future, the hope is that this will also allow us to perform resumable / incremental document parsing, by
 * simply invalidating any changed ParseState, and then restarting the parser in the correct state, over the changed set
 * of text.
 *
 */
public class ParseState {

    private ParseState parent;

    private final Node src;

    public ParseState(Node src) {
        this.src = src;
    }

    public Node getNext() {
        return src;
    }

    public void addChild(ParseState child) {
        child.parent = this;
    }

    public ParseState finish() {
        return parent;
    }

    public ParseState getParent() {
        return parent;
    }

    public Node getSrc() {
        return src;
    }
}
