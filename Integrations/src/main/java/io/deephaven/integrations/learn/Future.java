package io.deephaven.integrations.learn;
/*
// imports
import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

public class Future {

    private IndexSet indexSet;
    private boolean called;
    private Object result;
    private PyObject pyFunc;
    private PythonFunction funcCaller;

    public Future(PyObject pyFunc, int batchSize) {
        this.pyFunc = pyFunc;
        this.indexSet = new IndexSet(batchSize);
        this.called = False;
        this.result = null;
        this.funcCaller = new PythonFunction(this.pyFunc, ?);
    }

    public void clear() {
        this.result = null;
    }

    public Object get() {
        if (this.called) {
            this.result = this.funcCaller.apply(this.indexSet);
            this.indexSet.clear();
            this.called = True;
            this.pyFunc = null;
            this.indexSet = null;
        }
        return this.result;
    }

}
*/