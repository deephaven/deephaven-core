/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util;

import groovy.lang.GroovyRuntimeException;
import org.codehaus.groovy.GroovyException;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;

public class GroovyExceptionWrapper extends RuntimeException {

    public GroovyExceptionWrapper(@NotNull final Throwable original) {
        super(original.getMessage(), maybeTranslateCause(original));
        setStackTrace(original.getStackTrace());
    }

    private static Throwable maybeTranslateCause(@NotNull final Throwable original) {
        final Throwable originalCause = original.getCause();
        if (originalCause == null || originalCause == original) {
            return null;
        }
        return maybeTranslateGroovyException(originalCause);
    }

    public static Throwable maybeTranslateGroovyException(final Throwable original) {
        if (original instanceof GroovyException || original instanceof GroovyRuntimeException) {
            return new GroovyExceptionWrapper(original);
        }
        final Throwable replacementCause = maybeTranslateCause(original);
        if (replacementCause != original.getCause()) {
            return replaceWithNewCause(original, replacementCause);
        }
        return original;
    }

    /**
     * Returns a replacement for the original exception, except now wrapping the new cause, since the existing exception
     * can't be given a new cause.
     */
    private static Throwable replaceWithNewCause(final Throwable original, final Throwable replacementCause) {
        assert !(original instanceof GroovyException) && !(original instanceof GroovyRuntimeException);
        assert !(replacementCause instanceof GroovyException) && !(replacementCause instanceof GroovyRuntimeException);

        final Throwable replacement = makeReplacement(original, replacementCause);
        replacement.setStackTrace(original.getStackTrace());
        return replacement;
    }

    @NotNull
    private static Throwable makeReplacement(@NotNull final Throwable original, final Throwable replacementCause) {
        final Class<? extends Throwable> originalClass = original.getClass();
        if (original.getMessage() == null) {
            try {
                return originalClass.getConstructor(Throwable.class).newInstance(replacementCause);
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                    | NoSuchMethodException e1) {
                try {
                    final Throwable result = originalClass.newInstance();
                    result.initCause(replacementCause);
                    return result;
                } catch (InstantiationException | IllegalAccessException e2) {
                    return new TranslatedException(original.getClass().getName(), replacementCause);
                }
            }
        }
        try {
            return originalClass.getConstructor(String.class, Throwable.class).newInstance(original.getMessage(),
                    replacementCause);
        } catch (NoSuchMethodException | IllegalAccessException | InstantiationException
                | InvocationTargetException e1) {
            try {
                final Throwable result = originalClass.getConstructor(String.class).newInstance(original.getMessage());
                result.initCause(replacementCause);
                return result;
            } catch (NoSuchMethodException | IllegalAccessException | InstantiationException
                    | InvocationTargetException e2) {
                return new TranslatedException(original.getClass().getName() + ": " + original.getMessage(),
                        replacementCause);
            }
        }
    }

    public static class TranslatedException extends RuntimeException {

        public TranslatedException(final String s, final Throwable cause) {
            super(s, cause);
        }
    }
}
