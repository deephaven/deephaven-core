package io.deephaven.engine.context;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.NoExecutionContextRegisteredException;

import java.util.Collection;

public class PoisonedQueryLibrary extends QueryLibrary {
    private static final Logger logger = LoggerFactory.getLogger(PoisonedQueryScope.class);
    public static final PoisonedQueryLibrary INSTANCE = new PoisonedQueryLibrary();

    private PoisonedQueryLibrary() {}

    private <T> T fail() {
        logger.error().append(
                "No ExecutionContext provided, cannot use QueryLibrary. If this is being run in a thread, did you specify an ExecutionContext for the thread? Please refer to the documentation on ExecutionContext for details.")
                .endl();
        throw new NoExecutionContextRegisteredException();
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
