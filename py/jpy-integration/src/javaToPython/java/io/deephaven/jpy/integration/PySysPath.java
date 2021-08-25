package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyConfigExt;
import io.deephaven.jpy.JpyConfigSource.SysProps;
import java.util.List;
import java.util.stream.Collectors;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

public class PySysPath {
    public static void main(String[] args) {
        JpyConfigExt jpyConfig = new JpyConfigExt(SysProps.INSTANCE.asJpyConfig());

        System.out.println(jpyConfig);

        jpyConfig.initPython();
        jpyConfig.startPython();
        List<String> path = getSysPath();
        System.out.println(path.stream().collect(Collectors.joining(System.lineSeparator())));
        PyLib.stopPython();
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
