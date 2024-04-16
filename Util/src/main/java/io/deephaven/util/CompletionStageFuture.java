//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A safe version of CompletableFuture that does not expose the completion API.
 *
 * @param <T> The result type returned by this future's {@link #get()}
 */
public interface CompletionStageFuture<T> extends Future<T>, CompletionStage<T> {

    /**
     * Create a new incomplete future.
     *
     * @param <T> The result type returned by this future's {@link #get()}
     * @return a resolver for the future
     */
    static <T> Resolver<T> make() {
        return new CompletionStageFutureImpl<T>().new ResolverImpl();
    }

    /**
     * Returns a new CompletionStageFuture that is already completed with the given value.
     *
     * @param value the value
     * @param <U> the type of the value
     * @return the completed CompletionStageFuture
     * @see java.util.concurrent.CompletableFuture#completedFuture(Object)
     */
    static <U> CompletionStageFuture<U> completedFuture(U value) {
        final CompletionStageFutureImpl.Resolver<U> resolver = CompletionStageFuture.make();
        resolver.complete(value);
        return resolver.getFuture();
    }

    /**
     * Returns a new CompletionStageFuture that is already completed exceptionally with the given exception.
     *
     * @param ex the exception
     * @param <U> the type of the value
     * @return the exceptionally completed CompletionStageFuture
     * @see java.util.concurrent.CompletableFuture#failedFuture(Throwable)
     */
    static <U> CompletionStageFuture<U> failedFuture(Throwable ex) {
        final CompletionStageFutureImpl.Resolver<U> resolver = CompletionStageFuture.make();
        resolver.completeExceptionally(ex);
        return resolver.getFuture();
    }

    interface Resolver<T> {

        /**
         * If not already completed, sets the value returned by {@link #get()} and related methods to the given value.
         *
         * @param value the result value
         * @return {@code true} if this invocation caused this CompletionStageFuture to transition to a completed state,
         *         else {@code false}
         * @see java.util.concurrent.CompletableFuture#complete(Object)
         */
        boolean complete(T value);

        /**
         * If not already completed, causes invocations of {@link #get()} and related methods to throw the given
         * exception wrapped in an {@link ExecutionException}.
         *
         * @param ex the exception
         * @return {@code true} if this invocation caused this CompletionStageFuture to transition to a completed state,
         *         else {@code false}
         * @see java.util.concurrent.CompletableFuture#completeExceptionally(Throwable)
         */
        boolean completeExceptionally(@NotNull Throwable ex);

        /**
         * @return the underlying future to provide to the recipient; implementations must ensure that this method
         *         always returns an identical result for a given Resolver instance
         */
        CompletionStageFuture<T> getFuture();
    }

    @Override
    <U> CompletionStageFuture<U> thenApply(Function<? super T, ? extends U> fn);

    @Override
    <U> CompletionStageFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn);

    @Override
    <U> CompletionStageFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor);

    @Override
    CompletionStageFuture<Void> thenAccept(Consumer<? super T> action);

    @Override
    CompletionStageFuture<Void> thenAcceptAsync(Consumer<? super T> action);

    @Override
    CompletionStageFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor);

    @Override
    CompletionStageFuture<Void> thenRun(Runnable action);

    @Override
    CompletionStageFuture<Void> thenRunAsync(Runnable action);

    @Override
    CompletionStageFuture<Void> thenRunAsync(Runnable action, Executor executor);

    @Override
    <U, V> CompletionStageFuture<V> thenCombine(
            CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);

    @Override
    <U, V> CompletionStageFuture<V> thenCombineAsync(
            CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn);

    @Override
    <U, V> CompletionStageFuture<V> thenCombineAsync(
            CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor);

    @Override
    <U> CompletionStageFuture<Void> thenAcceptBoth(
            CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action);

    @Override
    <U> CompletionStageFuture<Void> thenAcceptBothAsync(
            CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action);

    @Override
    <U> CompletionStageFuture<Void> thenAcceptBothAsync(
            CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor);

    @Override
    CompletionStageFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action);

    @Override
    CompletionStageFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action);

    @Override
    CompletionStageFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor);

    @Override
    <U> CompletionStageFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn);

    @Override
    <U> CompletionStageFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn);

    @Override
    <U> CompletionStageFuture<U> applyToEitherAsync(
            CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor);

    @Override
    CompletionStageFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action);

    @Override
    CompletionStageFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action);

    @Override
    CompletionStageFuture<Void> acceptEitherAsync(
            CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor);

    @Override
    CompletionStageFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action);

    @Override
    CompletionStageFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action);

    @Override
    CompletionStageFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor);

    @Override
    <U> CompletionStageFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn);

    @Override
    <U> CompletionStageFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn);

    @Override
    <U> CompletionStageFuture<U> thenComposeAsync(
            Function<? super T, ? extends CompletionStage<U>> fn, Executor executor);

    @Override
    <U> CompletionStageFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn);

    @Override
    <U> CompletionStageFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn);

    @Override
    <U> CompletionStageFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor);

    @Override
    CompletionStageFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action);

    @Override
    CompletionStageFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action);

    @Override
    CompletionStageFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor);

    @Override
    CompletionStageFuture<T> exceptionally(Function<Throwable, ? extends T> fn);
}
