/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ide.lsp;

import com.google.gwt.core.client.JavaScriptObject;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
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

    @JsProperty
    public void setParameters(Object args) {
        if (args == null || args instanceof ParameterInformation[]) {
            parameters = (ParameterInformation[]) args;
        } else if (args instanceof JavaScriptObject) {
            // this is actually javascript. We can do terrible things here and it's ok
            final int length = Array.getLength(args);
            final ParameterInformation[] typed = new ParameterInformation[length];
            System.arraycopy(args, 0, typed, 0, length);
            parameters = JsObject.freeze(typed);
        } else {
            throw new IllegalArgumentException("Not a ParameterInformation[] or js []" + args);
        }
    }

    @JsProperty
    public Object getParameters() {
        return parameters;
    }

    @Override
    @JsIgnore
    public String toString() {
        return "SignatureInformation{" +
                "label=" + label +
                ", documentation=" + documentation +
                ", parameters=" + Arrays.toString(parameters) +
                ", activeParameter=" + activeParameter +
                "}\n";
    }
}
