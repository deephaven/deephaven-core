package io.deephaven.lang.completion;

import io.deephaven.db.tables.TableDefinition;

import java.util.Collection;

/**
 *
 */
public interface VariableProvider {
    Collection<String> getVariableNames();

    Class getVariableType(String var);

    Object getVariable(String var);

    TableDefinition getTableDefinition(String var);
}
