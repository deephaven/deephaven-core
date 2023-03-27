/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class ParameterInformation implements Serializable {
    public String label;
    public MarkupContent documentation;

    @Override
    @JsIgnore
    public String toString() {
        return "ParameterInformation{" +
                "label=" + label +
                ", documentation='" + documentation + '\'' +
                '}';
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final ParameterInformation paramInfo = (ParameterInformation) o;

        return label.equals(paramInfo.label) && documentation.equals(paramInfo.documentation);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = label.hashCode();
        result = 31 * result + documentation.hashCode();
        return result;
    }
}
