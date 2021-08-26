package io.deephaven.lang.api;

import io.deephaven.lang.generated.*;

import java.util.ArrayList;
import java.util.List;

/**
 * A common interface for {@link ChunkerIdent}, {@link ChunkerInvoke}, {@link ChunkerNew} and
 * {@link ChunkerArray} which are the only ast nodes that can be "scope objects":
 * thing.field.callMethod()[0].moreMethod.new SomeClass().why.would().you.new Ever()
 */
public interface IsScope extends Node {

    String getName();

    void setScopeTarget(Node target);

    Node getScopeTarget();

    List<IsScope> getScope();

    default List<IsScope> asScopeList() {
        List<IsScope> scopes = new ArrayList<>();
        scopes.addAll(getScope());
        scopes.add(this);
        return scopes;
    }
}
