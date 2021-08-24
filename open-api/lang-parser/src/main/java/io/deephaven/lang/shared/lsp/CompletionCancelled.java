package io.deephaven.lang.shared.lsp;

/**
 * Thrown from the document parser if the user has updated the document while an ongoing completion
 * request was blocking on stale input.
 *
 * This is used to fast-path quitting a completion request because the document was invalidated.
 *
 */
public class CompletionCancelled extends RuntimeException {

    public CompletionCancelled() {
        super("Completion request cancelled");
    }

}
