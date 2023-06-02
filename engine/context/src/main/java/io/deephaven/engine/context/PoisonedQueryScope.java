/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.util.ExecutionContextRegistrationException;

import java.util.Set;

public class PoisonedQueryScope extends QueryScope {

    public static final PoisonedQueryScope INSTANCE = new PoisonedQueryScope();

    private PoisonedQueryScope() {}

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("QueryScope");
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
