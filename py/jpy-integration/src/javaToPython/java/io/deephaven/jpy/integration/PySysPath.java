//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jpy.integration;

import io.deephaven.jpy.JpyConfigExt;
import io.deephaven.jpy.JpyConfigSource;
import java.util.List;
import java.util.stream.Collectors;
import org.jpy.PyLib;
import org.jpy.PyModule;
import org.jpy.PyObject;

public class PySysPath {
    public static void main(String[] args) {
        JpyConfigExt jpyConfig = new JpyConfigExt(JpyConfigSource.sysProps().asJpyConfig());

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
