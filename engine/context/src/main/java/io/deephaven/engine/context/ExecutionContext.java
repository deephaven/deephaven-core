/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.VisibleForTesting;

import java.util.Arrays;
import java.util.function.Supplier;

public class ExecutionContext {

    public static final ExecutionContext[] ZERO_LENGTH_EXECUTION_CONTEXT_ARRAY = new ExecutionContext[0];

    public static Builder newBuilder() {
        return new Builder();
    }

    public static ExecutionContext makeSystemicExecutionContext() {
        final ExecutionContext context = getContext();
        if (context.isSystemic) {
            return context;
        }
        return ExecutionContext.newBuilder()
                .captureMutableQueryScope()
                .captureQueryLibrary()
                .captureCompilerContext()
                .markSystemic()
                .build();
    }

    @VisibleForTesting
    public static SafeCloseable createForUnitTests() {
        return newBuilder()
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setCompilerContext(CompilerTools.createContextForUnitTests())
                .build()
                .open();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ThreadLocal Management
    // -----------------------------------------------------------------------------------------------------------------

    private static volatile ExecutionContext defaultContext = null;

    private static ExecutionContext getDefaultContext() {
        if (defaultContext == null) {
            synchronized (ExecutionContext.class) {
                if (defaultContext == null) {
                    defaultContext = newBuilder().markSystemic().build();
                }
            }
        }
        return defaultContext;
    }

    private static final ThreadLocal<ExecutionContext> currentContext =
            ThreadLocal.withInitial(ExecutionContext::getDefaultContext);

    /**
     * @return an object representing the current execution context or null if the current context is systemic
     */
    public static ExecutionContext getContextToRecord() {
        final ExecutionContext ctxt = currentContext.get();
        if (ctxt.isSystemic) {
            return null;
        }
        return ctxt;
    }

    /**
     * @return an object representing the current execution context
     */
    public static ExecutionContext getContext() {
        return currentContext.get();
    }

    /**
     * Installs the executionContext to be used for the current thread.
     */
    public static void setContext(final ExecutionContext context) {
        currentContext.set(context);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ExecutionContext Implementation
    // -----------------------------------------------------------------------------------------------------------------

    /** True if this execution context is supplied by the system and not the user. */
    private final boolean isSystemic;

    private final QueryLibrary.Context queryLibrary;
    private final QueryScope queryScope;
    private final CompilerTools.Context compilerContext;

    private ExecutionContext(
            final boolean isSystemic,
            final QueryLibrary.Context queryLibrary,
            final QueryScope queryScope,
            final CompilerTools.Context compilerContext) {
        this.isSystemic = isSystemic;
        this.queryLibrary = queryLibrary;
        this.queryScope = queryScope;
        this.compilerContext = compilerContext;
    }

    /**
     * Execute runnable within this execution context.
     */
    public void apply(Runnable runnable) {
        apply(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Executes supplier within this execution context.
     */
    public <T> T apply(Supplier<T> supplier) {
        try (SafeCloseable ignored = open()) {
            // actually evaluate the script
            return supplier.get();
        }
    }

    /**
     * Installs the executionContext and returns a function that will restore the original executionContext.
     *
     * @return a closeable to cleanup the execution context
     */
    public SafeCloseable open() {
        // save the current context
        final ExecutionContext oldExecutionContext = currentContext.get();

        ExecutionContext.setContext(this);

        return () -> {
            // restore the old context
            ExecutionContext.setContext(oldExecutionContext);
        };
    }

    public QueryLibrary.Context getQueryLibrary() {
        return queryLibrary;
    }

    public QueryScope getQueryScope() {
        return queryScope;
    }

    public CompilerTools.Context getCompilerContext() {
        return compilerContext;
    }

    public static class Builder {
        private boolean isSystemic = false;

        private QueryLibrary.Context queryLibrary = PoisonedQueryLibrary.INSTANCE;
        private QueryScope queryScope = PoisonedQueryScope.INSTANCE;
        private CompilerTools.Context compilerContext = PoisonedCompilerToolsContext.INSTANCE;

        public Builder markSystemic() {
            isSystemic = true;
            return this;
        }

        public Builder newQueryLibrary() {
            queryLibrary = QueryLibrary.makeNewLibrary();
            return this;
        }

        public Builder setQueryLibrary(final QueryLibrary.Context queryLibrary) {
            this.queryLibrary = queryLibrary;
            return this;
        }

        public Builder captureQueryLibrary() {
            queryLibrary = currentContext.get().getQueryLibrary();
            return this;
        }

        public Builder setCompilerContext(final CompilerTools.Context compilerContext) {
            this.compilerContext = compilerContext;
            return this;
        }

        public Builder captureCompilerContext() {
            compilerContext = currentContext.get().getCompilerContext();
            return this;
        }

        public Builder emptyQueryScope() {
            this.queryScope = EmptyQueryScope.INSTANCE;
            return this;
        }

        public Builder newQueryScope() {
            this.queryScope = new QueryScope.StandaloneImpl();
            return this;
        }

        public Builder setQueryScope(final QueryScope queryScope) {
            this.queryScope = queryScope;
            return this;
        }

        public Builder captureMutableQueryScope() {
            this.queryScope = currentContext.get().getQueryScope();
            return this;
        }

        public Builder captureQueryScopeVars(String... vars) {
            this.queryScope = new QueryScope.StandaloneImpl();

            final QueryScopeParam<?>[] params = QueryScope.getScope().getParams(Arrays.asList(vars));
            for (final QueryScopeParam<?> param : params) {
                this.queryScope.putParam(param.getName(), param.getValue());
            }

            return this;
        }

        public ExecutionContext build() {
            return new ExecutionContext(isSystemic, queryLibrary, queryScope, compilerContext);
        }
    }
}
