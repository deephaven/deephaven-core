package io.deephaven.web.shared.data;

import java.io.Serializable;

/**
 * Adds extra information necessary to treat a table as an InputTable instance.
 */
public class InputTableDefinition implements Serializable {

    private String[] keys;
    private String[] values;

    public String[] getKeys() {
        return keys;
    }

    public void setKeys(String[] keys) {
        this.keys = keys;
    }

    public String[] getValues() {
        return values;
    }

    public void setValues(String[] values) {
        this.values = values;
    }
}
