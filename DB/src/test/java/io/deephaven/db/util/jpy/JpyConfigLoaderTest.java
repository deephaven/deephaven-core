package io.deephaven.db.util.jpy;

import io.deephaven.configuration.Configuration;
import io.deephaven.jpy.JpyConfig;
import io.deephaven.jpy.JpyConfig.Flag;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class JpyConfigLoaderTest {

    private static final Path DEFAULT_PROGRAM_NAME = Paths.get("/programName");
    private static final Path DEFAULT_PYTHON_HOME = Paths.get("/pythonHome");
    private static final Path DEFAULT_PYTHON_LIB = Paths.get("/libpython.so");
    private static final Path DEFAULT_JPY_LIB = Paths.get("/jpy.so");
    private static final Path DEFAULT_JDL_LIB = Paths.get("/jdl.so");
    private static final List<Path> DEFAULT_EXTRA_PATHS = Collections.emptyList();
    private static final EnumSet<Flag> DEFAULT_FLAGS = EnumSet.noneOf(Flag.class);

    @Test
    public void configDefaults() {
        JpyConfig config = load("jpy-config-defaults.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            DEFAULT_PYTHON_HOME,
            DEFAULT_PYTHON_LIB,
            DEFAULT_JPY_LIB,
            DEFAULT_JDL_LIB,
            DEFAULT_EXTRA_PATHS,
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configBadParents() {
        try {
            load("jpy-config-bad-parents.prop");
            Assert.fail("Expected to throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("jpy lib and jdl lib must be siblings"));
        }
    }

    @Test
    public void configNullJdlLib() {
        JpyConfig config = load("jpy-config-null-jdlLib.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            DEFAULT_PYTHON_HOME,
            DEFAULT_PYTHON_LIB,
            DEFAULT_JPY_LIB,
            null,
            DEFAULT_EXTRA_PATHS,
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configNullJpyLib() {
        JpyConfig config = load("jpy-config-null-jpyLib.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            DEFAULT_PYTHON_HOME,
            DEFAULT_PYTHON_LIB,
            null,
            DEFAULT_JDL_LIB,
            DEFAULT_EXTRA_PATHS,
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configNullPythonLib() {
        JpyConfig config = load("jpy-config-null-pythonLib.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            DEFAULT_PYTHON_HOME,
            null,
            DEFAULT_JPY_LIB,
            DEFAULT_JDL_LIB,
            DEFAULT_EXTRA_PATHS,
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configNullPythonHome() {
        JpyConfig config = load("jpy-config-null-pythonHome.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            null,
            DEFAULT_PYTHON_LIB,
            DEFAULT_JPY_LIB,
            DEFAULT_JDL_LIB,
            DEFAULT_EXTRA_PATHS,
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configNullProgramName() {
        JpyConfig config = load("jpy-config-null-programName.prop");

        JpyConfig jpyConfig = new JpyConfig(
            null,
            DEFAULT_PYTHON_HOME,
            DEFAULT_PYTHON_LIB,
            DEFAULT_JPY_LIB,
            DEFAULT_JDL_LIB,
            DEFAULT_EXTRA_PATHS,
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configExtraPaths() {
        JpyConfig config = load("jpy-config-extra-paths.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            DEFAULT_PYTHON_HOME,
            DEFAULT_PYTHON_LIB,
            DEFAULT_JPY_LIB,
            DEFAULT_JDL_LIB,
            Arrays.asList(Paths.get("/e1"), Paths.get("/e2"), Paths.get("/e3")),
            DEFAULT_FLAGS);

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configFlags() {
        JpyConfig config = load("jpy-config-flags.prop");

        JpyConfig jpyConfig = new JpyConfig(
            DEFAULT_PROGRAM_NAME,
            DEFAULT_PYTHON_HOME,
            DEFAULT_PYTHON_LIB,
            DEFAULT_JPY_LIB,
            DEFAULT_JDL_LIB,
            DEFAULT_EXTRA_PATHS,
            EnumSet.of(Flag.MEM, Flag.EXEC, Flag.JVM));

        Assert.assertEquals(jpyConfig, config);
    }

    @Test
    public void configRelativeJdlLib() {
        try {
            load("jpy-config-relative-jdlLib.prop");
            Assert.fail("Expected to throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("jdlLib must be absolute"));
        }
    }

    @Test
    public void configRelativeJpyLib() {
        try {
            load("jpy-config-relative-jpyLib.prop");
            Assert.fail("Expected to throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("jpyLib must be absolute"));
        }
    }

    @Test
    public void configRelativePythonLib() {
        try {
            load("jpy-config-relative-pythonLib.prop");
            Assert.fail("Expected to throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("pythonLib must be absolute"));
        }
    }

    @Test
    public void configRelativePythonHome() {
        try {
            load("jpy-config-relative-pythonHome.prop");
            Assert.fail("Expected to throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("pythonHome must be absolute"));
        }
    }

    @Test
    public void configRelativeProgramName() {
        try {
            load("jpy-config-relative-programName.prop");
            Assert.fail("Expected to throw IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("programName must be absolute"));
        }
    }

    private static JpyConfig load(String resource) {
        // a bit hacky that we can't just pass resource as is, but we aren't allowed to pass our own
        // class
        // context for configuration loading...
        Configuration configuration = loadConfig(
            String.format("io/deephaven/db/util/jpy/%s", resource));
        return new JpyConfigLoader(configuration).asJpyConfig();
    }

    private static Configuration loadConfig(String configFile) {
        // todo: there should be a MUCH easier way to do this - very ugly b/c dependent on system
        // props,
        // and Configuration not an interface...
        String existingValue = System.getProperty("Configuration.rootFile");
        System.setProperty("Configuration.rootFile", configFile);
        Configuration config = Configuration.TEST_NEW_Configuration();
        if (existingValue != null) {
            System.setProperty("Configuration.rootFile", existingValue);
        } else {
            System.clearProperty("Configuration.rootFile");
        }
        return config;
    }
}
