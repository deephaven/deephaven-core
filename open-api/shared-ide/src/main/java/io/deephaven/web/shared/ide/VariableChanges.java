package io.deephaven.web.shared.ide;

import java.io.Serializable;

/**
 * Represents a set of C*UD operation results.
 */
public class VariableChanges implements Serializable {
    public VariableDefinition[] created;
    public VariableDefinition[] updated;
    public VariableDefinition[] removed;
}
