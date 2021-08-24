package io.deephaven.lang.completion;

import io.deephaven.lang.generated.ChunkerAssign;
import io.deephaven.lang.generated.ChunkerDocument;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.parse.api.ParsedResult;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.Position;

import java.util.Collection;

/**
 * General API for returning a list of completion results from a given offset in a source command.
 */
public interface CompletionHandler<ResultType extends ParsedResult<ChunkerDocument, ChunkerAssign, Node>> {
    Collection<CompletionItem.Builder> runCompletion(ResultType doc, Position pos, int offset);
}
