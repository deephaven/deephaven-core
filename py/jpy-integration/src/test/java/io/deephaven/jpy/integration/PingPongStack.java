package io.deephaven.jpy.integration;

import org.jpy.CreateModule;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jpy.PyLib;
import org.jpy.PyObject;

public class PingPongStack {

    private static String CODE = null;

    private static PyObject loadCodeAsModule(String code, String moduleName) {
        return CreateModule.create().call(moduleName, code);
    }

    private synchronized static void init() {
        if (CODE == null) {
            try {
                CODE = new String(
                    Files.readAllBytes(
                        Paths.get(PingPongStack.class.getResource("pingpongstack.py").toURI())),
                    StandardCharsets.UTF_8);
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static String pingPongPython(String result, int remaining) {
        if (!PyLib.isPythonRunning()) {
            throw new IllegalStateException("Expected python to already be running");
        }
        init();
        if (remaining <= 0) {
            return result;
        }
        // note: we can't cache this module here, since we are starting and stopping python
        // interpreter
        // from our junit testing framework, and don't have the appropriate infra to pass down the
        // env.
        // the same thing would likely happen from python->java, in regards to caching
        // jpy.get_type() among jvm create/destroys.

        PyObject module = loadCodeAsModule(CODE, "pingpongstack");
        return module.call("ping_pong_java", result + "(java," + remaining + ")", remaining - 1)
            .str();
    }
}
