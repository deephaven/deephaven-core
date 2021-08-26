package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionOptions;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.ChunkerAssign;
import io.deephaven.lang.generated.Token;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;

import java.util.Collection;
import java.util.Set;

/**
 * A class specifically for completing assignment statements; to be called after the completer has discovered the cursor
 * near an assignment token.
 *
 */
public class CompleteAssignment extends CompletionBuilder {

    private final ChunkerAssign assign;
    private final Token assignToken;

    public CompleteAssignment(ChunkerCompleter completer, ChunkerAssign assign) {
        super(completer);
        this.assign = assign;
        // This is non-free to compute, so do it once up front
        assignToken = assign.assignToken();
    }

    public void doCompletion(
            Collection<CompletionItem.Builder> results,
            CompletionRequest request,
            String varName,
            boolean methodMatched) {
        final CompletionOptions opts = new CompletionOptions().setPrevTokens("=", " ");
        if (assign.getValue() == null) {
            // There is no value after the =, so try adding a . or ( to complete the expression.
            // In truth, this could probably just be done at the call site by adding to the varName argument.
            opts.setNextTokens(methodMatched ? "(" : ".");
            // If the user has already typed a ( or ., then we definitely should not add either of them,
            // so we use stopTokens to tell #addMatch when to stop adding suffixes.
            opts.setStopTokens("(", ".");
        }
        addMatch(results, assignToken, assignToken.next, varName, request, opts);
    }
}
