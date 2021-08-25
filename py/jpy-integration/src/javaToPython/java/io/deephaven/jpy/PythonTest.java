package io.deephaven.jpy;

import io.deephaven.jpy.PythonResource.StartStopRule;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.jpy.CreateModule;
import org.jpy.PyLib;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Rule;

public abstract class PythonTest {

    @ClassRule
    public static final PythonResource PYTHON_RESOURCE = PythonResource.ofSysProps();

    @Rule
    public final StartStopRule START_STOP_RESOURCE = PYTHON_RESOURCE.startStopRule();

    public CreateModule getCreateModule() {
        return START_STOP_RESOURCE.getCreateModule();
    }

    public static void assumePython3() {
        Assume.assumeTrue(PyLib.getPythonVersion().startsWith("3"));
    }

    public static String readResource(Class<?> clazz, String name) {
        try {
            return new String(
                    Files.readAllBytes(Paths.get(clazz.getResource(name).toURI())),
                    StandardCharsets.UTF_8);
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
