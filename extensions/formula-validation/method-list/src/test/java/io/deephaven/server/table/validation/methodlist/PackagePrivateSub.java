//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

/**
 * A fixture for {@link TestMethodListInvocationValidator#testPackagePrivateOverrides()}.
 */
public class PackagePrivateSub extends PackagePrivateBase {
    @Override
    int packagePrivate() {
        return 2;
    }
}
