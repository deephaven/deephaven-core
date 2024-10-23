//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.event;

import com.vertispan.tsdefs.annotations.TsInterface;
import com.vertispan.tsdefs.annotations.TsName;
import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.dom.DomGlobal;
import elemental2.promise.Promise;
import io.deephaven.web.client.api.CoreClient;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.fu.RemoverFn;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;
import jsinterop.base.JsPropertyMap;

/**
 */
@TsInterface
@TsName(namespace = "dh")
public class HasEventHandling {
    public static final String INTERNAL_EVENT_RELEASED = "released-internal";

    private final JsPropertyMap<JsArray<EventFn<?>>> map = Js.uncheckedCast(JsObject.create(null));
    private boolean suppress = false;

    protected String logPrefix() {
        return "";
    }

    /**
     * Listen for events on this object.
     *
     * @param name the name of the event to listen for
     * @param callback a function to call when the event occurs
     * @return Returns a cleanup function.
     * @param <T> the type of the data that the event will provide
     */
    @JsMethod
    public <T> RemoverFn addEventListener(String name, EventFn<T> callback) {
        JsArray<EventFn<?>> listeners = map.get(name);
        if (listeners == null) {
            listeners = new JsArray<>(callback);
            map.set(name, listeners);
        } else {
            if (hasListener(name, callback)) {
                JsLog.warn(logPrefix() + "You are double-adding the callback " + name + " : ",
                        callback + ", removing old instance.");
                removeEventListener(name, callback);
            }
            listeners.push(callback);
        }
        return () -> removeEventListener(name, callback);
    }

    public <T> void addEventListenerOneShot(String name, EventFn<T> callback) {
        /*
         * Hack to workaround how GWT creates js functions and manages binding "this". The "self" instance is actually
         * _not_ the same object as "this", as it represents the JS Function instead of the Java instance - effectively,
         * "self" is something like this::onEvent.bind(this).
         */
        final class WrappedCallback implements EventFn<T> {
            private EventFn<T> self;

            @Override
            public void onEvent(Event<T> e) {
                removeEventListener(name, self);
                callback.onEvent(e);
            }
        }
        WrappedCallback fn = new WrappedCallback();
        fn.self = fn;
        addEventListener(name, fn);
    }

    public static class EventPair<T> {
        private String name;
        private EventFn<T> callback;

        public static <T> EventPair<T> of(String name, EventFn<T> callback) {
            final EventPair<T> pair = new EventPair<>();
            pair.name = name;
            pair.callback = callback;
            return pair;
        }
    }

    public void addEventListenerOneShot(EventPair<?>... pairs) {
        boolean[] seen = {false};
        for (EventPair<?> pair : pairs) {
            addEventListenerOneShot(pair.name, e -> {
                if (seen[0]) {
                    return;
                }
                seen[0] = true;
                pair.callback.onEvent((Event) e);
            });
        }
    }

    @JsMethod
    public <T> Promise<Event<T>> nextEvent(String eventName, @JsOptional Double timeoutInMillis) {
        LazyPromise<Event<T>> promise = new LazyPromise<>();

        addEventListenerOneShot(eventName, promise::succeed);

        if (timeoutInMillis != null) {
            return promise.asPromise((int) (double) timeoutInMillis);
        }
        return promise.asPromise();
    }

    @JsMethod
    public boolean hasListeners(String name) {
        final JsArray<EventFn<?>> listeners = map.get(name);
        return listeners != null && listeners.length > 0;
    }

    public boolean hasListener(String name, EventFn<?> fn) {
        return hasListeners(name) && map.get(name).indexOf(fn) != -1;
    }

    /**
     * Removes an event listener added to this table.
     *
     * @param name
     * @param callback
     * @return
     * @param <T>
     */
    @JsMethod
    public <T> boolean removeEventListener(String name, EventFn<T> callback) {
        final JsArray<EventFn<?>> listeners = map.get(name);
        if (listeners == null) {
            JsLog.warn(logPrefix() + "Asked to remove an event listener which wasn't present, ignoring.");
            return false;
        }
        int index = listeners.indexOf(callback);
        if (index == -1) {
            JsLog.warn(logPrefix()
                    + "Asked to remove an event listener which wasn't present, ignoring. Present listeners for that event: ",
                    listeners);
            return false;
        }
        // remove the item
        listeners.splice(index, 1);
        if (listeners.length == 0) {
            map.delete(name);
        }
        return true;
    }

    public void fireEvent(String type) {
        fireEvent(new Event<>(type, null));
    }

    public <T> void fireEvent(String type, @DoNotAutobox T detail) {
        fireEvent(new Event<>(type, detail));
    }

    public <T> void fireEvent(Event<T> e) {
        if (suppress) {
            JsLog.debug("Event suppressed", e.getType(), e);
            return;
        }
        if (map.has(e.getType())) {
            final JsArray<EventFn<T>> callbacks = Js.cast(JsArray.from((JsArrayLike<EventFn<?>>) map.get(e.getType())));
            callbacks.forEach((item, ind) -> {
                try {
                    item.onEvent(e);
                } catch (Throwable t) {
                    DomGlobal.console.error(logPrefix() + "User callback (", item, ") of type ", e.getType(),
                            " failed: ", t);
                    t.printStackTrace();
                }
                return true;
            });
        }
    }

    public <T> void fireCriticalEvent(String type) {
        if (hasListeners(type)) {
            fireEvent(type);
        } else {
            DomGlobal.console.error(logPrefix() + type, "(to prevent this log message, handle the " + type + " event)");
        }
    }

    public <T> void fireCriticalEvent(String type, T detail) {
        if (hasListeners(type)) {
            fireEvent(type, detail);
        } else {
            DomGlobal.console.error(logPrefix(), detail,
                    "(to prevent this log message, handle the " + type + " event)");
        }
    }

    public void failureHandled(String failure) {
        if (failure == null) {
            return;
        }
        fireCriticalEvent(CoreClient.EVENT_REQUEST_FAILED, failure);
    }

    public void suppressEvents() {
        suppress = true;
    }

    public void unsuppressEvents() {
        suppress = false;
    }

    public boolean isSuppress() {
        return suppress;
    }
}
