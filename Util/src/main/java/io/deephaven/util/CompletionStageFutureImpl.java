//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.*;

/**
 * A {@link CompletableFuture} that prevents users from completing the future by hiding behind
 * {@link CompletionStageFuture}.
 *
 * @param <T> The result type returned by this future's {@code join}
 */
@SuppressWarnings("unchecked")
public class CompletionStageFutureImpl<T> extends CompletableFuture<T> implements CompletionStageFuture<T> {

    /**
     * A resolver for this future implementation.
     */
    class ResolverImpl implements CompletionStageFuture.Resolver<T> {
        public boolean complete(final T value) {
            return safelyComplete(value);
        }

        public boolean completeExceptionally(@NotNull final Throwable ex) {
            return safelyCompleteExceptionally(ex);
        }

        public CompletionStageFuture<T> getFuture() {
            return CompletionStageFutureImpl.this;
        }
    }

    private boolean safelyComplete(final T value) {
        return super.complete(value);
    }

    private boolean safelyCompleteExceptionally(@NotNull final Throwable ex) {
        return super.completeExceptionally(ex);
    }

    @Override
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new CompletionStageFutureImpl<>();
    }

    ///////////////////////////
    // CompletableFuture API //
    ///////////////////////////

    @Override
    public boolean complete(final T value) {
        throw erroneousCompletionException();
    }

    @Override
    public boolean completeExceptionally(@NotNull final Throwable ex) {
        throw erroneousCompletionException();
    }

    @Override
    public void obtrudeValue(final T value) {
        throw erroneousCompletionException();
    }

    @Override
    public void obtrudeException(@NotNull final Throwable ex) {
        throw erroneousCompletionException();
    }

    @Override
    public CompletableFuture<T> completeAsync(
            @NotNull final Supplier<? extends T> supplier,
            @NotNull final Executor executor) {
        throw erroneousCompletionException();
    }

    @Override
    public CompletableFuture<T> completeAsync(@NotNull final Supplier<? extends T> supplier) {
        throw erroneousCompletionException();
    }

    @Override
    public CompletableFuture<T> completeOnTimeout(final T value, long timeout, TimeUnit unit) {
        throw erroneousCompletionException();
    }

    private static UnsupportedOperationException erroneousCompletionException() {
        return new UnsupportedOperationException("Users should not complete futures.");
    }

    /////////////////////////
    // CompletionStage API //
    /////////////////////////

    @Override
    public <U> CompletionStageFutureImpl<U> thenApply(@NotNull final Function<? super T, ? extends U> fn) {
        return (CompletionStageFutureImpl<U>) super.thenApply(fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> thenApplyAsync(@NotNull final Function<? super T, ? extends U> fn) {
        return (CompletionStageFutureImpl<U>) super.thenApplyAsync(fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> thenApplyAsync(
            @NotNull final Function<? super T, ? extends U> fn,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<U>) super.thenApplyAsync(fn, executor);
    }

    @Override
    public CompletionStageFutureImpl<Void> thenAccept(@NotNull final Consumer<? super T> action) {
        return (CompletionStageFutureImpl<Void>) super.thenAccept(action);
    }

    @Override
    public CompletionStageFutureImpl<Void> thenAcceptAsync(@NotNull final Consumer<? super T> action) {
        return (CompletionStageFutureImpl<Void>) super.thenAcceptAsync(action);
    }

    @Override
    public CompletionStageFutureImpl<Void> thenAcceptAsync(
            @NotNull final Consumer<? super T> action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<Void>) super.thenAcceptAsync(action, executor);
    }

    @Override
    public CompletionStageFutureImpl<Void> thenRun(@NotNull final Runnable action) {
        return (CompletionStageFutureImpl<Void>) super.thenRun(action);
    }

    @Override
    public CompletionStageFutureImpl<Void> thenRunAsync(@NotNull final Runnable action) {
        return (CompletionStageFutureImpl<Void>) super.thenRunAsync(action);
    }

    @Override
    public CompletionStageFutureImpl<Void> thenRunAsync(
            @NotNull final Runnable action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<Void>) super.thenRunAsync(action, executor);
    }

    @Override
    public <U, V> CompletionStageFutureImpl<V> thenCombine(
            @NotNull final CompletionStage<? extends U> other,
            @NotNull final BiFunction<? super T, ? super U, ? extends V> fn) {
        return (CompletionStageFutureImpl<V>) super.thenCombine(other, fn);
    }

    @Override
    public <U, V> CompletionStageFutureImpl<V> thenCombineAsync(
            @NotNull final CompletionStage<? extends U> other,
            @NotNull final BiFunction<? super T, ? super U, ? extends V> fn) {
        return (CompletionStageFutureImpl<V>) super.thenCombineAsync(other, fn);
    }

    @Override
    public <U, V> CompletionStageFutureImpl<V> thenCombineAsync(
            @NotNull final CompletionStage<? extends U> other,
            @NotNull final BiFunction<? super T, ? super U, ? extends V> fn,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<V>) super.thenCombineAsync(other, fn, executor);
    }


    @Override
    public <U> CompletionStageFutureImpl<Void> thenAcceptBoth(
            @NotNull final CompletionStage<? extends U> other,
            @NotNull final BiConsumer<? super T, ? super U> action) {
        return (CompletionStageFutureImpl<Void>) super.thenAcceptBoth(other, action);
    }


    @Override
    public <U> CompletionStageFutureImpl<Void> thenAcceptBothAsync(
            @NotNull final CompletionStage<? extends U> other,
            @NotNull final BiConsumer<? super T, ? super U> action) {
        return (CompletionStageFutureImpl<Void>) super.thenAcceptBothAsync(other, action);
    }

    @Override
    public <U> CompletionStageFutureImpl<Void> thenAcceptBothAsync(
            @NotNull final CompletionStage<? extends U> other,
            @NotNull final BiConsumer<? super T, ? super U> action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<Void>) super.thenAcceptBothAsync(other, action, executor);
    }


    @Override
    public CompletionStageFutureImpl<Void> runAfterBoth(
            @NotNull final CompletionStage<?> other,
            @NotNull final Runnable action) {
        return (CompletionStageFutureImpl<Void>) super.runAfterBoth(other, action);
    }

    @Override
    public CompletionStageFutureImpl<Void> runAfterBothAsync(
            @NotNull final CompletionStage<?> other,
            @NotNull final Runnable action) {
        return (CompletionStageFutureImpl<Void>) super.runAfterBothAsync(other, action);
    }

    @Override
    public CompletionStageFutureImpl<Void> runAfterBothAsync(
            @NotNull final CompletionStage<?> other,
            @NotNull final Runnable action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<Void>) super.runAfterBothAsync(other, action, executor);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> applyToEither(
            @NotNull final CompletionStage<? extends T> other,
            @NotNull final Function<? super T, U> fn) {
        return (CompletionStageFutureImpl<U>) super.applyToEither(other, fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> applyToEitherAsync(
            @NotNull final CompletionStage<? extends T> other,
            @NotNull final Function<? super T, U> fn) {
        return (CompletionStageFutureImpl<U>) super.applyToEitherAsync(other, fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> applyToEitherAsync(
            @NotNull final CompletionStage<? extends T> other,
            @NotNull final Function<? super T, U> fn, Executor executor) {
        return (CompletionStageFutureImpl<U>) super.applyToEitherAsync(other, fn, executor);
    }

    @Override
    public CompletionStageFutureImpl<Void> acceptEither(
            @NotNull final CompletionStage<? extends T> other,
            @NotNull final Consumer<? super T> action) {
        return (CompletionStageFutureImpl<Void>) super.acceptEither(other, action);
    }

    @Override
    public CompletionStageFutureImpl<Void> acceptEitherAsync(
            @NotNull final CompletionStage<? extends T> other,
            @NotNull final Consumer<? super T> action) {
        return (CompletionStageFutureImpl<Void>) super.acceptEitherAsync(other, action);
    }

    @Override
    public CompletionStageFutureImpl<Void> acceptEitherAsync(
            @NotNull final CompletionStage<? extends T> other,
            @NotNull final Consumer<? super T> action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<Void>) super.acceptEitherAsync(other, action, executor);
    }

    @Override
    public CompletionStageFutureImpl<Void> runAfterEither(
            @NotNull final CompletionStage<?> other,
            @NotNull final Runnable action) {
        return (CompletionStageFutureImpl<Void>) super.runAfterEither(other, action);
    }

    @Override
    public CompletionStageFutureImpl<Void> runAfterEitherAsync(
            @NotNull final CompletionStage<?> other,
            @NotNull final Runnable action) {
        return (CompletionStageFutureImpl<Void>) super.runAfterEitherAsync(other, action);
    }

    @Override
    public CompletionStageFutureImpl<Void> runAfterEitherAsync(
            @NotNull final CompletionStage<?> other,
            @NotNull final Runnable action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<Void>) super.runAfterEitherAsync(other, action, executor);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> thenCompose(
            @NotNull final Function<? super T, ? extends CompletionStage<U>> fn) {
        return (CompletionStageFutureImpl<U>) super.thenCompose(fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> thenComposeAsync(
            @NotNull final Function<? super T, ? extends CompletionStage<U>> fn) {
        return (CompletionStageFutureImpl<U>) super.thenComposeAsync(fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> thenComposeAsync(
            @NotNull final Function<? super T, ? extends CompletionStage<U>> fn,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<U>) super.thenComposeAsync(fn, executor);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> handle(@NotNull final BiFunction<? super T, Throwable, ? extends U> fn) {
        return (CompletionStageFutureImpl<U>) super.handle(fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> handleAsync(
            @NotNull final BiFunction<? super T, Throwable, ? extends U> fn) {
        return (CompletionStageFutureImpl<U>) super.handleAsync(fn);
    }

    @Override
    public <U> CompletionStageFutureImpl<U> handleAsync(
            @NotNull final BiFunction<? super T, Throwable, ? extends U> fn,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<U>) super.handleAsync(fn, executor);
    }

    @Override
    public CompletionStageFutureImpl<T> whenComplete(@NotNull final BiConsumer<? super T, ? super Throwable> action) {
        return (CompletionStageFutureImpl<T>) super.whenComplete(action);
    }

    @Override
    public CompletionStageFutureImpl<T> whenCompleteAsync(
            @NotNull final BiConsumer<? super T, ? super Throwable> action) {
        return (CompletionStageFutureImpl<T>) super.whenCompleteAsync(action);
    }

    @Override
    public CompletionStageFutureImpl<T> whenCompleteAsync(
            @NotNull final BiConsumer<? super T, ? super Throwable> action,
            @NotNull final Executor executor) {
        return (CompletionStageFutureImpl<T>) super.whenCompleteAsync(action, executor);
    }

    @Override
    public CompletionStageFutureImpl<T> exceptionally(@NotNull final Function<Throwable, ? extends T> fn) {
        return (CompletionStageFutureImpl<T>) super.exceptionally(fn);
    }
}
