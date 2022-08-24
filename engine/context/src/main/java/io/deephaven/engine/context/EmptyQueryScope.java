/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import java.util.Collections;
import java.util.Set;

public class EmptyQueryScope extends QueryScope {
    public final static EmptyQueryScope INSTANCE = new EmptyQueryScope();

    private EmptyQueryScope() {}

    @Override
    public Set<String> getParamNames() {
        return Collections.emptySet();
    }

    @Override
    public boolean hasParamName(String name) {
        return false;
    }

    @Override
    protected <T> QueryScopeParam<T> createParam(String name) throws MissingVariableException {
        throw new MissingVariableException("Missing variable " + name);
    }

    @Override
    public <T> T readParamValue(String name) throws MissingVariableException {
        throw new MissingVariableException("Missing variable " + name);
    }

    @Override
    public <T> T readParamValue(String name, T defaultValue) {
        return defaultValue;
    }

    @Override
    public <T> void putParam(String name, T value) {
        throw new IllegalStateException("EmptyQueryScope cannot create parameters");
    }

    @Override
    public void putObjectFields(Object object) {
        throw new IllegalStateException("EmptyQueryScope cannot create parameters");
    }
}
