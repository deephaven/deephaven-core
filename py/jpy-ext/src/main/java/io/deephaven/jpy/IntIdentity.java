package io.deephaven.jpy;

import org.jpy.CreateModule;
import org.jpy.IdentityModule;

public interface IntIdentity extends AutoCloseable {
    static IntIdentity create(CreateModule createModule) {
        return IdentityModule.create(createModule, IntIdentity.class);
    }

    int identity(int x);

    @Override
    void close();
}
