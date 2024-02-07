/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.util;

import io.deephaven.engine.table.TableDefinition;

import java.util.Set;

/**
 *
 */
public interface VariableProvider {
    Set<String> getVariableNames();

    Class<?> getVariableType(String var);

    <T> T getVariable(String var, T defaultValue);

    TableDefinition getTableDefinition(String var);
}