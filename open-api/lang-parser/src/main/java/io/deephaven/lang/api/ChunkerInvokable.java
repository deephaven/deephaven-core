package io.deephaven.lang.api;

import io.deephaven.lang.generated.ChunkerConstants;
import io.deephaven.lang.generated.ChunkerMethodName;
import io.deephaven.lang.generated.Node;
import io.deephaven.lang.generated.Token;

import java.util.List;

/**
 * Represents an ast node that could be invokable.
 *
 * For now, this is methods and constructors, but will likely be expanded to handle closures to some
 * degree as well.
 */
public interface ChunkerInvokable extends IsScope {

    void addArgument(Node argument);

    void addToken(Token token);

    default Token getNameToken() {
        final List<Node> kids = getChildren();
        for (int i = kids.size(); i-- > 0;) {
            final Node child = kids.get(i);
            if (child instanceof ChunkerMethodName) {
                return child.jjtGetFirstToken();
            } else if (child instanceof ChunkerInvokable) {
                return ((ChunkerInvokable) child).getNameToken();
            }
        }
        // this fallback shouldn't really be needed.
        assert false : "Invokable without a method name: " + this;
        return tokens(true)
            .filter(tok -> tok.kind == ChunkerConstants.INVOKE)
            .first();
    }
}
