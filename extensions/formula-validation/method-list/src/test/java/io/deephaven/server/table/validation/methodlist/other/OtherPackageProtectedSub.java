//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist.other;

import io.deephaven.server.table.validation.methodlist.TestMethodListInvocationValidator;

/**
 * A fixture for {@code TestMethodListInvocationValidator#testProtectedOverrides()}: a public override, from another
 * package, of a protected method.
 */
public class OtherPackageProtectedSub extends TestMethodListInvocationValidator.ProtectedBase {
    @Override
    public int prot() {
        return 2;
    }

    public int unrelated() {
        return 3;
    }
}
