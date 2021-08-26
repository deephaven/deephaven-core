package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.ChunkerInvoke;
import io.deephaven.lang.generated.Node;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;

import java.util.Set;
import java.util.stream.Stream;

/**
 * A class specifically for completing table namespaces; to be called after the completer has discovered the name of the
 * namespace.
 *
 */
public class CompleteTableNamespace extends CompletionBuilder {

    private final ChunkerInvoke invoke;
    private final Stream<String> matches;

    public CompleteTableNamespace(ChunkerCompleter completer, ChunkerInvoke invoke, Stream<String> matches) {
        super(completer);
        this.invoke = invoke;
        // we should probably change this argument from a Stream<String> to the `Node replaced`, so we can do the
        // replaceNode/placeAfter work here, once, and then invoke doCompletion N times for each item.
        // That is, we should move the loop out of this CompletionBuilder
        this.matches = matches;
    }

    public void doCompletion(
            Node replaced,
            Set<CompletionItem.Builder> results,
            CompletionRequest request) {
        final int argInd = invoke.indexOfArgument(replaced);
        final String qt = getCompleter().getQuoteType(replaced);
        // TODO: move the chunk of code below into constructor, since it's not necessary to repeat inside a loop.
        // IDS-1517-14
        if (argInd == 0 || argInd == -1) {
            // The cursor is on the table namespace argument. Replace the string node itself.
            final DocumentRange.Builder range;
            if (replaced == null) {
                range = placeAfter(invoke, request);
            } else {
                range = replaceNode(replaced, request);
            }
            matches.forEach(match -> {
                StringBuilder b = new StringBuilder();
                b.append(qt);
                b.append(match);
                b.append(qt);
                if (replaced == null) {
                    b.append(", ").append(qt);
                } else {
                    final String name = b.toString();
                    if (name.equals(replaced.toSource())) {
                        // This suggestion is a duplicate; remove it.
                        return;
                    }
                    addTokens(b, replaced.jjtGetLastToken(), ", ", qt);
                }
                final CompletionItem.Builder result = CompletionItem.newBuilder();
                String item = b.toString();
                result
                        .setStart(start)
                        .setLength(len)
                        .setLabel(item)
                        .getTextEditBuilder()
                        .setText(item)
                        .setRange(range);
                results.add(result);
            });
        } else if (argInd == 1) {
            // The cursor is on the table namespace argument. We'll need to replace the whole thing, plus add a little
            // suffix on
            matches.forEach(match -> {
                getCompleter().addMatch(results, replaced, match, request, qt, ")");
            });
        }
    }
}
