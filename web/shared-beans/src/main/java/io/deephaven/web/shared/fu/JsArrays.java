package io.deephaven.web.shared.fu;

import com.google.gwt.core.client.JavaScriptObject;
import elemental2.core.JsArray;
import elemental2.core.JsString;
import jsinterop.annotations.JsIgnore;
import jsinterop.base.Js;

import java.lang.reflect.Array;

public class JsArrays {
    @JsIgnore
    public static void setArray(Object args, JsConsumer<String[]> setter) {
        if (args == null || args instanceof String[]) {
            setter.apply((String[]) args);
        } else if (args instanceof JavaScriptObject) {
            // this is actually javascript. We can do terrible things here and it's ok
            final int length = Array.getLength(args);
            final String[] typed = new String[length];
            System.arraycopy(args, 0, typed, 0, length);
            setter.apply(typed);
        } else {
            throw new IllegalArgumentException("Not a String[] or js [] " + args);
        }
    }

    @JsIgnore
    public static String[] toStringArray(JsArray<JsString> jsArray) {
        String[] result = new String[jsArray.length];
        for (int i = 0; i < jsArray.length; i++) {
            result[i] = Js.cast(jsArray.getAt(i));
        }
        return result;
    }
}
