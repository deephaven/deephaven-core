//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist.other;

import io.deephaven.server.table.validation.methodlist.PackagePrivateBase;

/**
 * A fixture for {@code TestMethodListInvocationValidator#testPackagePrivateOverrides()}: a public method with the name
 * of a package-private method in another package, which does not override it.
 */
public class OtherPackageSub extends PackagePrivateBase {
    public int packagePrivate() {
        return 4;
    }
}
