//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.lang.completion;

import io.deephaven.lang.parse.ParsedDocument;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;
import io.deephaven.proto.backplane.script.grpc.Position;

import java.util.Collection;

/**
 * General API for returning a list of completion results from a given offset in a source command.
 */
public interface CompletionHandler {
    Collection<CompletionItem.Builder> runCompletion(ParsedDocument doc, Position pos, int offset);
}
