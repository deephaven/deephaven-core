/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegisteredException;

import java.util.Set;

public class PoisonedQueryScope extends QueryScope {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedQueryScope.class);
    public static final PoisonedQueryScope INSTANCE = new PoisonedQueryScope();

    private PoisonedQueryScope() {}

    private <T> T fail() {
        logger.error().append(
                "No ExecutionContext provided, cannot use QueryScope. If this is being run in a thread, did you specify an ExecutionContext for the thread? Please refer to the documentation on ExecutionContext for details.")
                .endl();
        throw new NoExecutionContextRegisteredException();
    }

    @Override
    public Set<String> getParamNames() {
        return fail();
    }

    @Override
    public boolean hasParamName(String name) {
        return fail();
    }

    @Override
    protected <T> QueryScopeParam<T> createParam(String name) throws MissingVariableException {
        return fail();
    }

    @Override
    public <T> T readParamValue(String name) throws MissingVariableException {
        return fail();
    }

    @Override
    public <T> T readParamValue(String name, T defaultValue) {
        return fail();
    }

    @Override
    public <T> void putParam(String name, T value) {
        fail();
    }

    @Override
    public void putObjectFields(Object object) {
        fail();
    }
}
