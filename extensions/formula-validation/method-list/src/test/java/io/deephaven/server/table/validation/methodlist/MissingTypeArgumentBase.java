//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

/**
 * A fixture for {@link TestMethodListInvocationValidator#testMissingGenericSignatureTypes()}.
 */
public class MissingTypeArgumentBase<T> {
    public void consume(final T value) {}
}
