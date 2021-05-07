package org.jpy;

import java.util.Objects;
import org.jpy.PyLib.CallableKind;

public class CreateModule implements AutoCloseable {

    // note: we are keeping the imports in the function to limit the scope needed

    private static final String PREFERRED = "def create_module(module_name, script):\n"
        + "  from importlib import util\n"
        + "  spec = util.spec_from_loader(module_name, loader=None)\n"
        + "  module = util.module_from_spec(spec)\n"
        + "  exec(script, module.__dict__)\n"
        + "  return module";

    private static final String DEPRECATED = "def create_module(module_name, script):\n"
        + "  import imp\n"
        + "  module = imp.new_module(module_name)\n"
        + "  exec(script, module.__dict__)\n"
        + "  return module";

    public static CreateModule create() {
        final String code = PyLib.getPythonVersion().startsWith("3") ? PREFERRED : DEPRECATED;
        try (
            final PyObject dict = PyObject.executeCode("dict()", PyInputMode.EXPRESSION);
            final PyObject exec = PyObject.executeCode(code, PyInputMode.SCRIPT, null, dict)) {
            return new CreateModule(dict.asDict().get("create_module"));
        }
    }

    private final PyObject function;

    private CreateModule(PyObject function) {
        this.function = Objects.requireNonNull(function, "function");
    }

    public PyObject call(String moduleName, String moduleScript) {
        return function.call("__call__", moduleName, moduleScript);
    }

    public <T> T callAsFunctionModule(String moduleName, String moduleScript, Class<T> clazz) {
        //noinspection unchecked
        return (T)call(moduleName, moduleScript).createProxy(CallableKind.FUNCTION, clazz);
    }

    public <T> T callAsMethodModule(String moduleName, String moduleScript, Class<T> clazz) {
        return call(moduleName, moduleScript).createProxy(clazz);
    }

    @Override
    public void close() {
        function.close();
    }
}
