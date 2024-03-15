//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.lang.completion;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.lang.api.IsScope;
import io.deephaven.lang.generated.ChunkerInvoke;
import io.deephaven.lang.generated.Node;
import io.deephaven.proto.backplane.script.grpc.CompletionItem;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Creates all specified implementation, and sends all queries to each as necessary.
 */
public class DelegatingCustomCompletion implements CustomCompletion {
    private final List<CustomCompletion> delegates;

    public DelegatingCustomCompletion(Set<Factory> factories) {
        delegates = factories.stream().map(Factory::create).collect(Collectors.toList());
    }

    @Override
    public void methodArgumentCompletion(ChunkerCompleter completer, ChunkerInvoke node, Node replaceNode,
            CompletionRequest request,
            ChunkerCompleter.SearchDirection direction, Collection<CompletionItem.Builder> results) {
        for (CustomCompletion delegate : delegates) {
            delegate.methodArgumentCompletion(completer, node, replaceNode, request, direction, results);
        }
    }

    @Override
    public Optional<Class<?>> resolveScopeType(IsScope scope) {
        for (CustomCompletion delegate : delegates) {
            Optional<Class<?>> result = delegate.resolveScopeType(scope);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    @Override
    public Optional<TableDefinition> resolveTableDefinition(ChunkerInvoke invoke, CompletionRequest offset) {
        for (CustomCompletion delegate : delegates) {
            Optional<TableDefinition> result = delegate.resolveTableDefinition(invoke, offset);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
}
