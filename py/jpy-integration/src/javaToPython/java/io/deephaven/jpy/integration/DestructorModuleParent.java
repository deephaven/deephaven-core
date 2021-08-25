package io.deephaven.jpy.integration;

import org.jpy.CreateModule;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import org.jpy.PyObject;

interface DestructorModuleParent extends AutoCloseable {

    static DestructorModuleParent create() {
        try (final CreateModule createModule = CreateModule.create()) {
            return create(createModule);
        }
    }

    static DestructorModuleParent create(CreateModule createModule) {
        final String code = readResource("destructor_test.py");
        try (
                final PyObject module = createModule.call("destructor_module", code)) {
            return module
                    .call("Parent")
                    .createProxy(DestructorModuleParent.class);
        }
    }

    PyObject create_child(OnDelete r);

    @Override
    void close();

    static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(
                            DestructorModuleParent.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    class OnDelete {

        private final CountDownLatch latch;

        OnDelete(CountDownLatch latch) {
            this.latch = latch;
        }

        // todo: this *doesn't* actually get mapped to a PyCallable_Check b/c the code looks like:
        /**
         * int PyCallable_Check(PyObject *x) { if (x == NULL) return 0; return Py_TYPE(x)->tp_call != NULL; }
         */

        public void __call__() {
            latch.countDown();
        }
    }
}
