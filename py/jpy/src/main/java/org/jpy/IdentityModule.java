package org.jpy;

public interface IdentityModule extends AutoCloseable {

    String IDENTITY_CODE = "def identity(x):\n  return x";

    static IdentityModule create(CreateModule createModule) {
        return create(createModule, IdentityModule.class);
    }

    static <T> T create(CreateModule createModule, Class<T> clazz) {
        return createModule.callAsFunctionModule("identity_module", IDENTITY_CODE, clazz);
    }

    PyObject identity(Object object);

    @Override
    void close();
}
