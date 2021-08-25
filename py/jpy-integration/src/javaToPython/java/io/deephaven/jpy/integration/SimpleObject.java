package io.deephaven.jpy.integration;

import org.jpy.CreateModule;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jpy.PyObject;

interface SimpleObject extends AutoCloseable {

    static PyObject create(CreateModule createModule) {
        final String code = readResource("simple_object.py");
        try (final PyObject module = createModule.call("simple_object_module", code)) {
            return module.call("SimpleObject");
        }
    }

    static String readResource(String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(
                            SimpleObject.class.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
