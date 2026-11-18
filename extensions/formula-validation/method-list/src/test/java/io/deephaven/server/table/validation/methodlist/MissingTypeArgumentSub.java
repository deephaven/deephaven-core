//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.validation.methodlist;

import java.util.List;

/**
 * A fixture for {@link TestMethodListInvocationValidator#testMissingGenericSignatureTypes()}: the type argument of its
 * superclass mentions {@link MissingTypeArgument}, while the erased signature of its override does not.
 */
public class MissingTypeArgumentSub extends MissingTypeArgumentBase<List<MissingTypeArgument>> {
    @Override
    public void consume(final List<MissingTypeArgument> value) {}
}
