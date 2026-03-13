//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.process;

import io.deephaven.properties.PropertyVisitorStringBase;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * This visitor converts the property key-value pairs into log entries that break out type, key, and value.
 */
public abstract class ProcessInfoLogVisitor extends PropertyVisitorStringBase {
    @Override
    public void visit(final String key, String value) {
        final int ix1 = key.indexOf('.');
        final String type1 = key.substring(0, ix1);
        final String remaining = key.substring(ix1 + 1);
        final int ix2 = remaining.indexOf('.');
        if (ix2 == -1) {
            try {
                log(type1, remaining, value);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return;
        }
        final String type2 = remaining.substring(0, ix2);
        final String remaining2 = remaining.substring(ix2 + 1);
        try {
            log(type1 + "." + type2, remaining2, value);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected abstract void log(final String type, final String key, final String value) throws IOException;
}
