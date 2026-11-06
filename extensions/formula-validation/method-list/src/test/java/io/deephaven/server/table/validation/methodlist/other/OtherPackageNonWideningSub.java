//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist.other;

import io.deephaven.server.table.validation.methodlist.PackagePrivateSub;

/**
 * A fixture for {@code TestMethodListInvocationValidator#testPackagePrivateOverrides()}: a public method in another
 * package with the name of a package-private method whose only override is itself package-private, so it overrides
 * neither.
 */
public class OtherPackageNonWideningSub extends PackagePrivateSub {
    public int packagePrivate() {
        return 6;
    }
}
