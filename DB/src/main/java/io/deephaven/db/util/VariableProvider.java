package io.deephaven.db.util;

import io.deephaven.db.tables.TableDefinition;

import java.util.Set;

/**
 *
 */
public interface VariableProvider {
    Set<String> getVariableNames();

    Class getVariableType(String var);

    <T> T getVariable(String var, T defaultValue);

    TableDefinition getTableDefinition(String var);
}
