/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ide.lsp;

import com.google.gwt.core.client.JavaScriptObject;
import elemental2.core.JsArray;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Objects;

import static io.deephaven.web.shared.fu.JsArrays.setArray;

@JsType(namespace = "dh.lsp")
public class SignatureInformation implements Serializable {
    public String label;
    public MarkupContent documentation;
    private ParameterInformation[] parameters;
    public int activeParameter;

    public SignatureInformation() {}

    @JsProperty
    public void setParameters(Object args) {
        if (args == null || args instanceof ParameterInformation[]) {
            parameters = (ParameterInformation[]) args;
        } else if (args instanceof JavaScriptObject) {
            // this is actually javascript. We can do terrible things here and it's ok
            final int length = Array.getLength(args);
            final ParameterInformation[] typed = new ParameterInformation[length];
            System.arraycopy(args, 0, typed, 0, length);
            parameters = typed;
        } else {
            throw new IllegalArgumentException("Not a ParameterInformation[] or js []" + args);
        }
    }

    @JsProperty
    public Object getParameters() {
        return parameters; // Should this clone the array?
    }

    @Override
    @JsIgnore
    public String toString() {
        return "SignatureInformation{" +
                "label=" + label +
                ", documentation=" + documentation +
                ", parameters=" + parameters +
                ", activeParameter=" + activeParameter +
                "}\n";
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final SignatureInformation that = (SignatureInformation) o;

        if (label != that.label)
            return false;
        if (documentation != that.documentation)
            return false;
        if (activeParameter != that.activeParameter)
            return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(parameters, that.parameters);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = label.hashCode();
        result = 31 * result + documentation.hashCode();
        result = 31 * result + activeParameter;
        result = 31 * result + Arrays.hashCode(parameters);
        return result;
    }
}
