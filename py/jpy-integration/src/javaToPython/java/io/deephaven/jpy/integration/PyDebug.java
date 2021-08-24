package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyConfigExt;
import io.deephaven.jpy.JpyConfigSource.SysProps;
import java.util.List;
import java.util.stream.Collectors;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

public class PyDebug {
    private static final boolean DEBUG = Boolean.getBoolean("jpy.debug");

    private static void debug(String log) {
        if (DEBUG)
            System.out.println(log);
    }

    public static void main(String[] args) {

        JpyConfigExt jpyConfigExt = new JpyConfigExt(SysProps.INSTANCE.asJpyConfig());
        jpyConfigExt.initPython();
        debug("Calling JpyConfig.startPython()...");
        jpyConfigExt.startPython();
        debug("Called JpyConfig.startPython()");

        debug("Called PyLib.isPythonRunning() = " + PyLib.isPythonRunning());

        List<String> path = getSysPath();
        System.out.println("sys.path");
        System.out.println(path.stream().collect(Collectors.joining(System.lineSeparator())));

        debug("Calling PyLib.stopPython()...");
        PyLib.stopPython();
        debug("Called PyLib.stopPython()");

        debug("Called PyLib.isPythonRunning() = " + PyLib.isPythonRunning());
    }

    private static List<String> getSysPath() {
        try (
            final PyModule sys = PyModule.importModule("sys");
            final PyObject path = sys.getAttribute("path")) {
            return path
                .asList()
                .stream()
                .map(PyObject::getStringValue)
                .collect(Collectors.toList());
        }
    }
}
