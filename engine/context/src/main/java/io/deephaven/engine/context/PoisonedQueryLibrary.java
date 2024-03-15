//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

import io.deephaven.util.ExecutionContextRegistrationException;

import java.util.Collection;

public class PoisonedQueryLibrary extends QueryLibrary {

    public static final PoisonedQueryLibrary INSTANCE = new PoisonedQueryLibrary();

    private PoisonedQueryLibrary() {}

    private <T> T fail() {
        throw ExecutionContextRegistrationException.onFailedComponentAccess("QueryLibrary");
    }

    @Override
    public void updateVersionString() {
        fail();
    }

    @Override
    public Collection<String> getImportStrings() {
        return fail();
    }

    @Override
    public Collection<Package> getPackageImports() {
        return fail();
    }

    @Override
    public Collection<Class<?>> getClassImports() {
        return fail();
    }

    @Override
    public Collection<Class<?>> getStaticImports() {
        return fail();
    }

    @Override
    public void importPackage(Package aPackage) {
        fail();
    }

    @Override
    public void importClass(Class<?> aClass) {
        fail();
    }

    @Override
    public void importStatic(Class<?> aClass) {
        fail();
    }
}
