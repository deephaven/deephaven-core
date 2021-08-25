package io.deephaven.lang.api;

/**
 * Thrown from the parser if the thread interrupt status is set.
 *
 * We don't want to deal with a checked exception from generated code, so we'll just use this, which can be thrown
 * freely.
 */
public class ParseCancelled extends RuntimeException {
}
