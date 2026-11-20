//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.util.List;

/**
 * A fixture for {@link TestMethodListInvocationValidator#testMissingGenericSignatureTypes()}: the generic return type
 * mentions {@link MissingTypeArgument}, while the erased return type does not.
 */
public class MissingReturnTypeBase {
    public List<MissingTypeArgument> produce() {
        return null;
    }
}
