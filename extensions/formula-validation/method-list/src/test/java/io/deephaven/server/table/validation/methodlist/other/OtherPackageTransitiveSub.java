//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist.other;

import io.deephaven.server.table.validation.methodlist.PackagePrivatePublicSub;

/**
 * A fixture for {@code TestMethodListInvocationValidator#testPackagePrivateOverrides()}: a method in another package
 * that overrides a package-private method transitively, through a public override in the package-private method's
 * package.
 */
public class OtherPackageTransitiveSub extends PackagePrivatePublicSub {
    @Override
    public int packagePrivate() {
        return 5;
    }
}
