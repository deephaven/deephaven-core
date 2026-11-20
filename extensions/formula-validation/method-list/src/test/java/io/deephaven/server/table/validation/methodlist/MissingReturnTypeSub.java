//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.util.List;

/**
 * A fixture for {@link TestMethodListInvocationValidator#testMissingGenericSignatureTypes()}.
 */
public class MissingReturnTypeSub extends MissingReturnTypeBase {
    @Override
    public List<MissingTypeArgument> produce() {
        return null;
    }
}
