/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

import java.io.Serializable;

@JsType(namespace = "dh.lsp")
public class Hover implements Serializable {
    public MarkupContent contents;
    public DocumentRange range;



    @Override
    @JsIgnore
    public String toString() {
        return "Hover{" +
                "contents=" + contents +
                ", range='" + range.toString() + '\'' +
                '}';
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final Hover paramInfo = (Hover) o;

        return contents.equals(paramInfo.contents) && range.equals(paramInfo.range);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = contents.hashCode();
        result = 31 * result + range.hashCode();
        return result;
    }
}
