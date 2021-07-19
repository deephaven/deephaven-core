package io.deephaven.integrations.learn;
/*
import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

public class Scatterer {

    private final int batchSize;
    private int count;
    private PyObject scatterFunc;
    private PythonFunction funcCaller;

    public Scatterer(int batchSize, PyObject scatterFunc) {
        this.batchSize = batchSize;
        this.count = -1;
        this.scatterFunc = scatterFunc;
        this.funcCaller = new PythonFunction(this.scatterFunc, ?);
    }

    public void clear() {
        this.count = -1;
    }

    public Object scatter(PyObject data) {
        this.count += 1;
        int offset = this.count % this.batchSize;
        return this.funcCaller.apply(data);
    }

}
*/