package io.deephaven.lang.api;

import io.deephaven.lang.generated.Node;

import java.util.List;

/**
 */
public interface HasScope extends Node {
    void setScope(List<IsScope> scope);

    List<IsScope> getScope();

    @Override
    default void addScope(List<IsScope> scope) {
        if (scope.isEmpty()) {
            return;
        }
        final List<IsScope> curScope = getScope();
        IsScope prevScope = null;
        Node target = curScope.isEmpty() ? this : curScope.get(curScope.size() - 1);
        curScope.addAll(0, scope);
        for (int i = scope.size(); i-- > 0;) {
            final IsScope item = scope.get(i);
            insertChild(item, 0);
            if (target != this) {
                target.addScope(item);
            } else {
                if (prevScope == null) {
                    item.setScopeTarget(target);
                } else {
                    item.setScopeTarget(prevScope);
                }
            }
            target = prevScope = item;
        }
    }

}
