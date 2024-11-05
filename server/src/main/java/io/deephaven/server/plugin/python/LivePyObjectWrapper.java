//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.python;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.ReferenceCountedLivenessReferent;
import io.deephaven.util.annotations.ScriptApi;
import org.jetbrains.annotations.NotNull;
import org.jpy.PyObject;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * Provides a mapping between Python refcount and Deephaven's liveness mechanism, allowing liveness scopes to manage the
 * single <a href='https://docs.python.org/3/glossary.html#term-strong-reference'>strong reference</a> that the PyObject
 * instance represents. This way, as long as PyObjectRefCountedNode instances are managed correctly by their parent
 * scope, the PyObject strong reference will be correctly released when the object is no longer used.
 * <p>
 * </p>
 * This class is experimental, and may be changed or moved in a future release to a new package.
 *
 * @see <a href="https://github.com/deephaven/deephaven-core/issues/1775">deephaven-core#1775</a>
 */
public final class LivePyObjectWrapper extends ReferenceCountedLivenessReferent {
    private final PyObject pythonObject;

    /**
     * Constructs a PyObjectRefCountedNode instance by wrapping a PyObject. The caller should ensure that an appropriate
     * liveness scope is open to retain this instance.
     */
    @ScriptApi // Called by internal Python code
    public LivePyObjectWrapper(@NotNull PyObject pythonObject) {
        this.pythonObject = pythonObject;
        LivenessScopeStack.peek().manage(this);
    }

    @OverridingMethodsMustInvokeSuper
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LivePyObjectWrapper that = (LivePyObjectWrapper) o;

        return pythonObject.equals(that.pythonObject);
    }

    @Override
    public int hashCode() {
        return pythonObject.hashCode();
    }
}
