package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionOptions;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.ChunkerConstants;
import io.deephaven.lang.generated.Token;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;

import java.util.Collection;
import java.util.Set;

/**
 * A class specifically for completing variable names.
 *
 */
public class CompleteVarName extends CompletionBuilder {

    private final Token replacing;

    public CompleteVarName(ChunkerCompleter completer, Token replacing) {
        super(completer);
        // This is non-free to compute, so do it once up front
        this.replacing = replacing;
    }

    public void doCompletion(
        Collection<CompletionItem.Builder> results,
        CompletionRequest request,
        String varName) {
        final CompletionOptions opts = new CompletionOptions();
        final Token before = replacing.prev();
        // if the user put a space before a =, make sure we add one after, if it is missing
        if (before != null && before.kind == ChunkerConstants.ASSIGN) {
            if (before.prev() != null && before.prev().kind == ChunkerConstants.WHITESPACE) {
                opts.setPrevTokens(" ");
            }
        }
        addMatch(results, replacing, replacing, varName, request, opts);
    }
}
