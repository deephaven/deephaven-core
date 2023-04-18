/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Any;
import jsinterop.base.Js;

/**
 * Common interface for various ways of accessing table data and formatting.
 *
 * Java note: this interface contains some extra overloads that aren't available in JS. Implementations are expected to
 * implement only abstract methods, and default methods present in this interface will dispatch accordingly.
 */
@TsName(namespace = "dh")
public interface TableData {
    @JsProperty
    JsArray<Column> getColumns();

    @JsProperty
    JsArray<? extends Row> getRows();

    @JsMethod
    default Row get(Object index) {
        if (index instanceof LongWrapper) {
            return get(((LongWrapper) index).getWrapped());
        }
        return get(Js.coerceToInt(index));
    }

    Row get(long index);

    Row get(int index);

    @JsMethod
    default Any getData(Object index, Column column) {
        if (index instanceof LongWrapper) {
            return getData(((LongWrapper) index).getWrapped(), column);
        }
        return getData(Js.coerceToInt(index), column);
    }

    Any getData(int index, Column column);

    Any getData(long index, Column column);

    @JsMethod
    default Format getFormat(Object index, Column column) {
        if (index instanceof LongWrapper) {
            return getFormat(((LongWrapper) index).getWrapped(), column);
        }
        return getFormat(Js.coerceToInt(index), column);
    }

    Format getFormat(int index, Column column);

    Format getFormat(long index, Column column);

    @TsName(namespace = "dh")
    public interface Row {
        @JsProperty
        LongWrapper getIndex();

        @JsMethod
        Any get(Column column);

        @JsMethod
        Format getFormat(Column column);
    }
}
