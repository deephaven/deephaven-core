/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.lang;

import io.deephaven.util.ExecutionContext;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;

import java.util.Set;

public class PoisonedQueryScope extends QueryScope {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedQueryScope.class);

    @Override
    public Set<String> getParamNames() {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }

    @Override
    public boolean hasParamName(String name) {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }

    @Override
    protected <T> QueryScopeParam<T> createParam(String name) throws MissingVariableException {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }

    @Override
    public <T> T readParamValue(String name) throws MissingVariableException {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }

    @Override
    public <T> T readParamValue(String name, T defaultValue) {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }

    @Override
    public <T> void putParam(String name, T value) {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }

    @Override
    public void putObjectFields(Object object) {
        logger.error().append("No ExecutionContext provided; cannot use QueryScope").endl();
        throw new ExecutionContext.NotRegistered();
    }
}
