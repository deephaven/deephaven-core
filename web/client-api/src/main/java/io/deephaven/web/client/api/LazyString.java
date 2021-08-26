package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import io.deephaven.web.shared.fu.JsProvider;

/**
 * Useful for logging; toString calls a supplier so you can easily hide expensive toString() behind a lambda
 */
public class LazyString {
    private final JsProvider<Object> expensiveThing;

    public LazyString(JsProvider<Object> expensiveThing) {
        this.expensiveThing = expensiveThing;
    }

    @SuppressWarnings("unchecked")
    public static LazyString of(Object obj) {
        if (obj instanceof JsProvider) {
            return new LazyString((JsProvider) obj);
        }
        return new LazyString(() -> obj);
    }

    public static LazyString of(JsProvider<Object> obj) {
        return new LazyString(obj);
    }

    @Override
    public String toString() {
        return String.valueOf(expensiveThing.valueOf());
    }

    public static Object[] resolve(Object[] msgs) {
        JsLazy<Object[]> copy = JsLazy.of(JsArray::of, msgs);
        for (int i = msgs.length; i-- > 0;) {
            if (msgs[i] instanceof LazyString) {
                // first time in will copy all the values, as is, including current LazyString
                final Object[] cp = copy.get();
                // mutate the resolved copy
                cp[i] = msgs[i].toString();
            }
        }
        // return the resolved copy if we bothered to make one.
        return copy.isAvailable() ? copy.get() : msgs;
    }
}
