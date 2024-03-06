//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy.integration;

import org.jpy.CreateModule;
import org.jpy.IdentityModule;

public interface IdentityOut extends AutoCloseable {

    static <T extends IdentityOut> T create(CreateModule createModule, Class<T> clazz) {
        return IdentityModule.create(createModule, clazz);
    }

    @Override
    void close();
}
