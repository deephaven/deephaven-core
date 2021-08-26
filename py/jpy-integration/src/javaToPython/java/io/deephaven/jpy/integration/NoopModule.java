package io.deephaven.jpy.integration;

import org.jpy.CreateModule;
import io.deephaven.jpy.integration.SomeJavaClassOutTest.SomeJavaClass;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jpy.PyObject;

interface NoopModule extends AutoCloseable {

    String NOOP_CODE = "def noop(obj):\n  pass";

    static NoopModule create(CreateModule createModule) {
        return createModule.callAsFunctionModule("noop_module", NOOP_CODE, NoopModule.class);
    }

    void noop(int object);

    void noop(Integer object);

    void noop(String object);

    void noop(SomeJavaClass object);

    void noop(int[] object);

    void noop(Integer[] object);

    void noop(String[] object);

    void noop(SomeJavaClass[] object);

    void noop(Object genericObject);

    void noop(PyObject pyObject);

    @Override
    void close();

    static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(
                            NoopModule.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
