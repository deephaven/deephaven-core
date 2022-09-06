/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.context;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
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
                .captureQueryScope()
                .captureQueryLibrary()
                .captureQueryCompiler()
                .markSystemic()
                .build();
    }

    @VisibleForTesting
    public static ExecutionContext createForUnitTests() {
        return newBuilder()
                .markSystemic()
                .newQueryScope()
                .newQueryLibrary()
                .setQueryCompiler(QueryCompiler.createForUnitTests())
                .build();
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ThreadLocal Management
    // -----------------------------------------------------------------------------------------------------------------

    private static volatile ExecutionContext defaultContext = null;

    public static ExecutionContext getDefaultContext() {
        ExecutionContext localContext;
        if ((localContext = defaultContext) == null) {
            synchronized (ExecutionContext.class) {
                if ((localContext = defaultContext) == null) {
                    localContext = defaultContext = newBuilder().markSystemic().build();
                }
            }
        }
        return localContext;
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
    static void setContext(final ExecutionContext context) {
        currentContext.set(context);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ExecutionContext Implementation
    // -----------------------------------------------------------------------------------------------------------------

    /** True if this execution context is supplied by the system and not the user. */
    private final boolean isSystemic;

    private final QueryLibrary queryLibrary;
    private final QueryScope queryScope;
    private final QueryCompiler queryCompiler;

    private ExecutionContext(
            final boolean isSystemic,
            final QueryLibrary queryLibrary,
            final QueryScope queryScope,
            final QueryCompiler queryCompiler) {
        this.isSystemic = isSystemic;
        this.queryLibrary = queryLibrary;
        this.queryScope = queryScope;
        this.queryCompiler = queryCompiler;
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

    public QueryLibrary getQueryLibrary() {
        return queryLibrary;
    }

    public QueryScope getQueryScope() {
        return queryScope;
    }

    public QueryCompiler getQueryCompiler() {
        return queryCompiler;
    }

    @SuppressWarnings("unused")
    public static class Builder {
        private boolean isSystemic = false;

        private QueryLibrary queryLibrary = PoisonedQueryLibrary.INSTANCE;
        private QueryScope queryScope = PoisonedQueryScope.INSTANCE;
        private QueryCompiler queryCompiler = PoisonedQueryCompiler.INSTANCE;

        /**
         * A systemic execution context is one that is supplied by the system and not the user. A systemic context will
         * not be recorded when work is deferred.
         */
        @ScriptApi
        public Builder markSystemic() {
            isSystemic = true;
            return this;
        }

        /**
         * Instantiate an empty query library.
         */
        @ScriptApi
        public Builder newQueryLibrary() {
            queryLibrary = QueryLibrary.makeNewLibrary();
            return this;
        }

        /**
         * Instantiate an empty query library using the provided libraryVersion. This is useful for testing.
         */
        @ScriptApi
        public Builder newQueryLibrary(String libraryVersion) {
            queryLibrary = QueryLibrary.makeNewLibrary(libraryVersion);
            return this;
        }

        /**
         * Use the provided QueryLibrary.
         */
        @ScriptApi
        public Builder setQueryLibrary(final QueryLibrary queryLibrary) {
            this.queryLibrary = queryLibrary;
            return this;
        }

        /**
         * Use the current ExecutionContext's QueryLibrary instance.
         */
        @ScriptApi
        public Builder captureQueryLibrary() {
            queryLibrary = currentContext.get().getQueryLibrary();
            return this;
        }

        /**
         * Use the provided QueryCompiler
         */
        @ScriptApi
        public Builder setQueryCompiler(final QueryCompiler queryCompiler) {
            this.queryCompiler = queryCompiler;
            return this;
        }

        /**
         * Use the current ExecutionContext's QueryCompiler instance.
         */
        @ScriptApi
        public Builder captureQueryCompiler() {
            queryCompiler = currentContext.get().getQueryCompiler();
            return this;
        }

        /**
         * Use a query scope that is immutably empty.
         */
        @ScriptApi
        public Builder emptyQueryScope() {
            this.queryScope = EmptyQueryScope.INSTANCE;
            return this;
        }

        /**
         * Instantiate a new query scope.
         */
        @ScriptApi
        public Builder newQueryScope() {
            this.queryScope = new QueryScope.StandaloneImpl();
            return this;
        }

        /**
         * Use the provided QueryScope.
         */
        @ScriptApi
        public Builder setQueryScope(final QueryScope queryScope) {
            this.queryScope = queryScope;
            return this;
        }

        /**
         * Use the current ExecutionContext's QueryScope instance.
         * <p>
         *
         * @apiNote This captures a reference to the current, likely mutable, query scope state. Future changes to the
         *          query scope may cause deferred operations to fail. Additionally, there is risk of GC leakage should
         *          the query scope be otherwise unreachable.
         */
        @ScriptApi
        public Builder captureQueryScope() {
            this.queryScope = currentContext.get().getQueryScope();
            return this;
        }

        /**
         * Instantiate a new QueryScope and initialize with values from the current ExecutionContext.
         *
         * @param vars the variable names to copy from the current ExecutionContext's QueryScope
         */
        @ScriptApi
        public Builder captureQueryScopeVars(String... vars) {
            if (vars.length == 0) {
                return newQueryScope();
            }

            this.queryScope = new QueryScope.StandaloneImpl();

            final QueryScopeParam<?>[] params = getContext().getQueryScope().getParams(Arrays.asList(vars));
            for (final QueryScopeParam<?> param : params) {
                this.queryScope.putParam(param.getName(), param.getValue());
            }

            return this;
        }

        /**
         * @return the newly instantiated ExecutionContext
         */
        @ScriptApi
        public ExecutionContext build() {
            return new ExecutionContext(isSystemic, queryLibrary, queryScope, queryCompiler);
        }
    }
}
