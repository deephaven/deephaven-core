/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ide;

import java.io.Serializable;

public class VariableDefinition implements Serializable {
    private String name;
    private String type;

    public VariableDefinition() {}

    public VariableDefinition(String name, String type) {
        this();

        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
