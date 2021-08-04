package io.deephaven.api.expression;

import io.deephaven.api.RawString;
import io.deephaven.api.Selectable;
import io.deephaven.api.value.Value;

import java.io.Serializable;

/**
 * Represents an evaluate-able expression structure.
 *
 * @see Selectable
 */
public interface Expression extends Serializable {

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        // TODO (deephaven-core#830): Add more table api Expression structuring

        void visit(Value value);

        void visit(RawString rawString);
    }
}
