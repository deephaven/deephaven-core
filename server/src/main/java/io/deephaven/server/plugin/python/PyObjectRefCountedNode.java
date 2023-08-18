package io.deephaven.server.plugin.python;

import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import org.jpy.PyObject;

/**
 *
 */
public class PyObjectRefCountedNode extends ReferenceCountedLivenessNode {
    private final PyObject pythonObject;

    public PyObjectRefCountedNode(PyObject pythonObject) {
        super(true);
        this.pythonObject = pythonObject;
    }

    @Override
    protected void destroy() {
        super.destroy();
        pythonObject.close();
    }

    public PyObject getPythonObject() {
        return pythonObject;
    }
}
