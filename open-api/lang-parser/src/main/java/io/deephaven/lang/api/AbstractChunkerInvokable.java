//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.lang.api;

import io.deephaven.lang.generated.Chunker;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.generated.ScopedNode;
import io.deephaven.lang.generated.Token;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 */
public abstract class AbstractChunkerInvokable extends ScopedNode implements ChunkerInvokable {

    private final List<Node> arguments;
    private Node scopeTarget;

    public AbstractChunkerInvokable(int i) {
        super(i);
        this.arguments = new ArrayList<>();
    }

    public AbstractChunkerInvokable(Chunker p, int i) {
        super(p, i);
        this.arguments = new ArrayList<>();
    }

    @Override
    public final void addArgument(Node argument) {
        arguments.add(argument);
    }

    @Override
    public final void addToken(Token token) {
        addToken(token, null);
    }

    public List<Node> getArguments() {
        return Collections.unmodifiableList(arguments);
    }

    public Node getArgument(int i) {
        return arguments.size() > i ? arguments.get(i) : null;
    }

    public int getArgumentCount() {
        return arguments.size();
    }

    @Override
    public Node getScopeTarget() {
        return scopeTarget;
    }

    @Override
    public void setScopeTarget(Node scopeTarget) {
        this.scopeTarget = scopeTarget;
    }
}
