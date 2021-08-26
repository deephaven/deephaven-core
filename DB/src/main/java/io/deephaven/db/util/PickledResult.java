package io.deephaven.db.util;

import java.io.Serializable;

/**
 * Pickled result for a Python fetch.
 */
public class PickledResult implements Serializable {
    private String pickled;
    private String pythonVersion; // version of python used to perform pickle

    PickledResult(String pickled, String pythonVersion) {
        this.pickled = pickled;
        this.pythonVersion = pythonVersion;
    }

    public String getPickled() {
        return pickled;
    }

    public String getPythonVersion() {
        return pythonVersion;
    }
}
