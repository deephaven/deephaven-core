package io.deephaven.engine.table.lang;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegistered;

import java.util.Collection;

public class PoisonedQueryLibrary implements QueryLibrary.Context {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedQueryScope.class);

    @Override
    public void updateVersionString() {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public Collection<String> getImportStrings() {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public Collection<Package> getPackageImports() {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public Collection<Class<?>> getClassImports() {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public Collection<Class<?>> getStaticImports() {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public void importPackage(Package aPackage) {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public void importClass(Class<?> aClass) {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }

    @Override
    public void importStatic(Class<?> aClass) {
        logger.error().append("No ExecutionContext provided; cannot use QueryLibrary").endl();
        throw new NoExecutionContextRegistered();
    }
}
