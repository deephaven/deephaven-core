/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.liveness;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.Utils;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * <p>
 * Support for a thread-local stack of {@link LivenessScope}s to allow the preferred programming model for scoping of
 * {@link LivenessArtifact}s.
 * <p>
 * Instances expect to be used on exactly one thread, and hence do not take any measures to ensure thread safety.
 */
public class LivenessScopeStack {

    private static final ThreadLocal<LivenessScopeStack> THREAD_STACK =
            ThreadLocal.withInitial(LivenessScopeStack::new);

    private static final ThreadLocal<LivenessManager> THREAD_BASE_MANAGER =
            ThreadLocal.withInitial(PermanentLivenessManager::new);

    private final Deque<LivenessScope> stack = new ArrayDeque<>();

    private LivenessScopeStack() {}

    /**
     * <p>
     * Push a scope onto the current thread's scope stack.
     *
     * @param scope The scope
     */
    public static void push(@NotNull final LivenessScope scope) {
        THREAD_STACK.get().pushInternal(scope);
    }

    /**
     * <p>
     * Pop a scope from the current thread's scope stack.
     * <p>
     * Must be the current top of the stack.
     *
     * @param scope The scope
     */
    public static void pop(@NotNull final LivenessScope scope) {
        THREAD_STACK.get().popInternal(scope);
    }

    /**
     * <p>
     * Get the scope at the top of the current thread's scope stack, or the base manager if no scopes have been pushed
     * but not popped on this thread.
     * <p>
     * This method defines the manager that should be used for all new {@link LivenessArtifact}s.
     *
     * @return The current manager
     */
    @NotNull
    public static LivenessManager peek() {
        return THREAD_STACK.get().peekInternal();
    }

    /**
     * <p>
     * Push a scope onto the scope stack, and get an {@link SafeCloseable} that pops it.
     * <p>
     * This is useful for enclosing scope usage in a try-with-resources block.
     *
     * @param scope The scope
     * @param releaseOnClose Whether the scope should be released when the result is closed
     * @return A {@link SafeCloseable} whose {@link SafeCloseable#close()} method invokes {@link #pop(LivenessScope)}
     *         for the scope (followed by {@link LivenessScope#release()} if releaseOnClose is true)
     */
    @NotNull
    public static SafeCloseable open(@NotNull final LivenessScope scope, final boolean releaseOnClose) {
        push(scope);
        return releaseOnClose ? new PopAndReleaseOnClose(scope) : new PopOnClose(scope);
    }

    /**
     * <p>
     * Push an anonymous scope onto the scope stack, and get an {@link SafeCloseable} that pops it and then
     * {@link LivenessScope#release()}s it.
     * <p>
     * This is useful enclosing a series of query engine actions whose results must be explicitly retained externally in
     * order to preserve liveness.
     *
     * @return A {@link SafeCloseable} whose {@link SafeCloseable#close()} method invokes {@link #pop(LivenessScope)}
     *         for the scope, followed by {@link LivenessScope#release()}
     */
    @NotNull
    public static SafeCloseable open() {
        final LivenessScope scope = new LivenessScope();
        push(scope);
        return new PopAndReleaseOnClose(scope);
    }

    private void pushInternal(@NotNull final LivenessScope scope) {
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: Pushing scope ").append(Utils.REFERENT_FORMATTER, scope).endl();
        }
        stack.push(scope);
    }

    private void popInternal(@NotNull final LivenessScope scope) {
        if (Liveness.DEBUG_MODE_ENABLED) {
            Liveness.log.info().append("LivenessDebug: Popping scope ").append(Utils.REFERENT_FORMATTER, scope).endl();
        }
        final LivenessScope peeked = stack.peekFirst();
        if (peeked != scope) {
            throw new IllegalStateException(
                    "Caller requested to pop " + scope + " but the top of the scope stack is " + peeked);
        }
        stack.pop();
    }

    @NotNull
    private LivenessManager peekInternal() {
        final LivenessScope peeked = stack.peekFirst();
        return peeked != null ? peeked : THREAD_BASE_MANAGER.get();
    }

    private static final class PopOnClose implements SafeCloseable {

        private final LivenessScope scope;

        private PopOnClose(@NotNull final LivenessScope scope) {
            this.scope = scope;
        }

        @Override
        public void close() {
            pop(scope);
        }
    }

    private static final class PopAndReleaseOnClose implements SafeCloseable {

        private final LivenessScope scope;

        private PopAndReleaseOnClose(@NotNull final LivenessScope scope) {
            this.scope = scope;
        }

        @Override
        public void close() {
            pop(scope);
            scope.release();
        }
    }

    /**
     * Perform a computation guarded by a new {@link LivenessScope} that is released before this method returns. The
     * result of the computation is managed by the enclosing {@link LivenessManager}, as determined by {@link #peek()}.
     *
     * @param computation The computation to perform. Will be invoked exactly once.
     * @param shouldEnclose Whether its actually necessary to use a new LivenessScope for the computation. Will be
     *        invoked exactly once.
     * @param shouldManageResult Whether its necessary to manage the result with the enclosing LivenessScope. Will be
     *        invoked exactly once.
     * @return The result of {@code computation.get()}
     */
    public static <RESULT_TYPE extends LivenessReferent> RESULT_TYPE computeEnclosed(
            @NotNull final Supplier<RESULT_TYPE> computation,
            @NotNull final BooleanSupplier shouldEnclose,
            @NotNull final Predicate<RESULT_TYPE> shouldManageResult) {
        final boolean manage = shouldEnclose.getAsBoolean();
        final LivenessManager enclosingLivenessManager = LivenessScopeStack.peek();
        try (final SafeCloseable ignored = manage ? LivenessScopeStack.open() : null) {
            final RESULT_TYPE result = computation.get();
            if (shouldManageResult.test(result)) {
                enclosingLivenessManager.manage(result);
            }
            return result;
        }
    }

    /**
     * Perform a computation guarded by a new {@link LivenessScope} that is released before this method returns. The
     * results of the computation are managed by the enclosing {@link LivenessManager}, as determined by
     * {@link #peek()}.
     *
     * @param computation The computation to perform. Will be invoked exactly once.
     * @param shouldEnclose Whether its actually necessary to use a new LivenessScope for the computation. Will be
     *        invoked exactly once.
     * @param shouldManageResult Whether its necessary to manage the result with the enclosing LivenessScope. Will be
     *        invoked exactly once per result.
     * @return The results of {@code computation.get()}
     */
    public static <RESULT_TYPE extends LivenessReferent> RESULT_TYPE[] computeArrayEnclosed(
            @NotNull final Supplier<RESULT_TYPE[]> computation,
            @NotNull final BooleanSupplier shouldEnclose,
            @NotNull final Predicate<RESULT_TYPE> shouldManageResult) {
        final boolean manage = shouldEnclose.getAsBoolean();
        final LivenessManager enclosingLivenessManager = LivenessScopeStack.peek();
        try (final SafeCloseable ignored = manage ? LivenessScopeStack.open() : null) {
            final RESULT_TYPE[] results = computation.get();
            for (final RESULT_TYPE result : results) {
                if (shouldManageResult.test(result)) {
                    enclosingLivenessManager.manage(result);
                }
            }
            return results;
        }
    }
}
