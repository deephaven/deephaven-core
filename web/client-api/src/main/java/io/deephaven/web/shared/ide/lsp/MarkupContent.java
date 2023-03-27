/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.ide.lsp;

import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

import java.io.Serializable;
import java.util.Objects;

@JsType(namespace = "dh.lsp")
public class MarkupContent implements Serializable {
    public String kind;
    public String value;

    public MarkupContent() {}

    @JsIgnore
    public MarkupContent(MarkupContent source) {
        this();
        this.kind = source.kind;
        this.value = source.value;
    }

    @Override
    @JsIgnore
    public String toString() {
        return "MarkupContent{" +
                "kind=" + kind +
                ", value=" + value +
                '}';
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final MarkupContent content = (MarkupContent) o;

        return kind.equals(content.kind) && value.equals(content.value);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        int result = kind.hashCode();
        result = 31 * result + value.hashCode();
        return result;
    }
}
