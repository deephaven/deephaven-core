package io.deephaven.server.plugin.python;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

/**
 * Provides a mapping between Python refcount and Deephaven's liveness mechanism, allowing liveness scopes to manage the
 * single <a href='https://docs.python.org/3/glossary.html#term-strong-reference'>strong reference</a> that the PyObject
 * instance represents. This way, as long as PyObjectRefCountedNode instances are managed correctly by their parent
 * scope, the PyObject strong reference will be correctly released when the object is no longer used.
 */
public class PyObjectRefCountedNode extends LivenessArtifact {
    private final PyObject pythonObject;

    /**
     * Constructs a PyObjectRefCountedNode instance by wrapping a PyObject. The caller should ensure that an appropriate
     * liveness scope is open to retain this instance.
     */
    @ScriptApi // Called by internal Python code
    public PyObjectRefCountedNode(PyObject pythonObject) {
        super(true);
        this.pythonObject = pythonObject;
    }

    @Override
    protected void destroy() {
        super.destroy();
        pythonObject.close();
    }

    /**
     * Returns the PyObject instance tracked by this object.
     */
    @ScriptApi // Called by internal Python code
    public PyObject getPythonObject() {
        return pythonObject;
    }
}
