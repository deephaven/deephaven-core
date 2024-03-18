//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy;

import org.jpy.CreateModule;
import org.jpy.IdentityModule;
import org.jpy.PyObject;

public interface PyObjectIdentity extends AutoCloseable {
    static PyObjectIdentity create(CreateModule createModule) {
        return IdentityModule.create(createModule, PyObjectIdentity.class);
    }

    PyObject identity(int x);

    @Override
    void close();
}
