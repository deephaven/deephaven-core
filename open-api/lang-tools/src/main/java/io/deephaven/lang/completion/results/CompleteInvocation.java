package io.deephaven.lang.completion.results;

import io.deephaven.lang.completion.ChunkerCompleter;
import io.deephaven.lang.completion.CompletionRequest;
import io.deephaven.lang.generated.ChunkerConstants;
import io.deephaven.lang.generated.Token;
import io.deephaven.function.BytePrimitives;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.DocumentRange;

import java.lang.reflect.Method;
import java.util.Collection;

/**
 * A class specifically for completing invocations; to be called with method results when the cursor is somewhere that a
 * method is valid.
 *
 */
public class CompleteInvocation extends CompletionBuilder {

    private final Token replacing;

    public CompleteInvocation(ChunkerCompleter completer, Token replacing) {
        super(completer);
        this.replacing = replacing;
    }

    public void doCompletion(Collection<CompletionItem.Builder> results, CompletionRequest request, Method method) {
        final int start = replacing.getStartIndex();
        final int length = replacing.getEndIndex() - start;

        StringBuilder res = new StringBuilder();
        final DocumentRange.Builder range;
        if (replacing.kind == ChunkerConstants.ACCESS) {
            res.append(".");
        }
        Token next = replacing.next;

        if (next != null) {
            if (next.kind == ChunkerConstants.ACCESS) {
                res.append(replacing.image);
                if (next.next != null) {
                    switch (next.next.kind) {
                        case ChunkerConstants.INVOKE:
                        case ChunkerConstants.ID:
                            res.append(".");
                            next = next.next;

                    }
                }
            }
            switch (next.kind) {
                case ChunkerConstants.ACCESS:
                    res.append(".");
                case ChunkerConstants.INVOKE:
                case ChunkerConstants.ID:
                    range = replaceTokens(replacing, next, request);
                    break;
                default:
                    range = replaceToken(replacing, request);
            }
        } else {
            range = replaceToken(replacing, request);
        }
        final String displayCompletion;
        if (method.getDeclaringClass().getSimpleName().endsWith("Primitives") &&
                BytePrimitives.class.getPackage().equals(method.getDeclaringClass().getPackage())) {
            // reduce massive duplication from same-named primitives methods.
            // In the future, when we have better column/type inference, we should be able to delete this workaround
            displayCompletion = "*Primitives." + method.getName() + "(";
        } else {
            displayCompletion = method.getDeclaringClass().getSimpleName() + "." + method.getName() + "(";
        }
        res.append(method.getName()).append("(");
        CompletionItem.Builder result =
                CompletionItem.newBuilder()
                        .setStart(start)
                        .setLength(length)
                        // let the user know where this method is coming from (include class name in display
                        // completion);
                        .setLabel(displayCompletion);
        result.getTextEditBuilder()
                .setText(res.toString())
                .setRange(range);
        // in the future, we should enable adding
        // explicit import statements for static methods. For now, we're assuming all static methods
        // already came from imports, but we'll want to handle this explicitly for more exotic cases in the future.
        results.add(result);
    }
}
