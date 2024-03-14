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
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Allows applications to offer custom autocomplete suggestions. Methods are all default with no implementation, so that
 * new operations can be added later.
 */
public interface CustomCompletion {

    /**
     * Factory interface for CustomCompletion instances, allowing the autocomplete internals to manage scope directly of
     * CustomCompletion instances. By implementing the Factory interface, scope of dependencies can be managed, but any
     * state should be left in the CustomCompletion itself (cached lookups, etc).
     */
    interface Factory {
        CustomCompletion create();
    }

    /**
     * User's cursor is within the method arguments, provide autocomplete suggestions for cursor position.
     */
    default void methodArgumentCompletion(ChunkerCompleter completer,
            ChunkerInvoke node,
            Node replaceNode,
            CompletionRequest request,
            ChunkerCompleter.SearchDirection direction,
            Collection<CompletionItem.Builder> results) {}

    /**
     * Return the type of the scoped value if known.
     */
    default Optional<Class<?>> resolveScopeType(IsScope scope) {
        return Optional.empty();
    }

    /**
     * Returns the definition of the table that would be created by the method call if known.
     */
    default Optional<TableDefinition> resolveTableDefinition(ChunkerInvoke invoke, CompletionRequest result) {
        return Optional.empty();
    }
}
