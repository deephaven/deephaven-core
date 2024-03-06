//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy.integration;

import io.deephaven.jpy.PythonTest;
import org.jpy.IdentityModule;
import org.jpy.PyObject;
import org.junit.Test;

public class TypeTest extends PythonTest {

    @Test
    public void checkReferenceCount() {
        try (
                final ReferenceCounting ref = ReferenceCounting.create(getCreateModule());
                final IdentityModule identity = IdentityModule.create(getCreateModule());
                final PyObject pyObject = SimpleObject.create(getCreateModule());
                final PyObject type = pyObject.getType()) {

            // It's hard for me to be more precise about this - jpy, and python itself, might be
            // keeping their own references to the type for lookup purposes.
            final int startingRefCount = ref.getLogicalRefCount(type);

            ref.check(startingRefCount, type);

            try (final PyObject anotherRef = identity.identity(type)) {
                // should increase the type by one
                ref.check(startingRefCount + 1, type);
            }
            ref.check(startingRefCount, type);

        }
    }
}
