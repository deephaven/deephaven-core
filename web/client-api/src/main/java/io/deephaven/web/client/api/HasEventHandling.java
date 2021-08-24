package io.deephaven.web.client.api;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.dom.CustomEvent;
import elemental2.dom.CustomEventInit;
import elemental2.dom.DomGlobal;
import elemental2.dom.Event;
import elemental2.promise.Promise;
import io.deephaven.web.client.fu.JsLog;
import io.deephaven.web.client.fu.LazyPromise;
import io.deephaven.web.shared.fu.RemoverFn;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsProperty;
import jsinterop.base.Js;
import jsinterop.base.JsArrayLike;
import jsinterop.base.JsPropertyMap;

/**
 */
public class HasEventHandling {

    @JsProperty(namespace = "dh.Client")
    public static final String EVENT_REQUEST_FAILED = "requestfailed";
    @JsProperty(namespace = "dh.Client")
    public static final String EVENT_REQUEST_STARTED = "requeststarted";
    @JsProperty(namespace = "dh.Client")
    public static final String EVENT_REQUEST_SUCCEEDED = "requestsucceeded";

    public static final String INTERNAL_EVENT_RELEASED = "released-internal";

    private final JsPropertyMap<JsArray<EventFn>> map = Js.uncheckedCast(JsObject.create(null));
    private boolean suppress = false;

    protected String logPrefix() {
        return "";
    }

    @JsMethod
    public RemoverFn addEventListener(String name, EventFn callback) {
        JsArray<EventFn> listeners = map.get(name);
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

    public void addEventListenerOneShot(String name, EventFn callback) {
        /*
         * Hack to workaround how GWT creates js functions and manages binding "this". The "self"
         * instance is actually _not_ the same object as "this", as it represents the JS Function
         * instead of the Java instance - effectively, "self" is something like
         * this::onEvent.bind(this).
         */
        final class WrappedCallback implements EventFn {
            private EventFn self;

            @Override
            public void onEvent(Event e) {
                removeEventListener(name, self);
                callback.onEvent(e);
            }
        }
        WrappedCallback fn = new WrappedCallback();
        fn.self = fn;
        addEventListener(name, fn);
    }

    public static class EventPair {
        private String name;
        private EventFn callback;

        public static EventPair of(String name, EventFn callback) {
            final EventPair pair = new EventPair();
            pair.name = name;
            pair.callback = callback;
            return pair;
        }
    }

    public void addEventListenerOneShot(EventPair... pairs) {
        boolean[] seen = {false};
        for (EventPair pair : pairs) {
            addEventListenerOneShot(pair.name, e -> {
                if (seen[0]) {
                    return;
                }
                seen[0] = true;
                pair.callback.onEvent(e);
            });
        }
    }

    @JsMethod
    public Promise<Event> nextEvent(String eventName, @JsOptional Double timeoutInMillis) {
        LazyPromise<Event> promise = new LazyPromise<>();

        addEventListenerOneShot(eventName, promise::succeed);

        if (timeoutInMillis != null) {
            return promise.asPromise((int) (double) timeoutInMillis);
        }
        return promise.asPromise();
    }

    @JsMethod
    public boolean hasListeners(String name) {
        final JsArray<EventFn> listeners = map.get(name);
        return listeners != null && listeners.length > 0;
    }

    public boolean hasListener(String name, EventFn fn) {
        return hasListeners(name) && map.get(name).indexOf(fn) != -1;
    }

    @JsMethod
    public boolean removeEventListener(String name, EventFn callback) {
        final JsArray<EventFn> listeners = map.get(name);
        if (listeners == null) {
            JsLog.warn(
                logPrefix() + "Asked to remove an event listener which wasn't present, ignoring.");
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
        fireEvent(type, CustomEventInit.create());
    }

    public void fireEventWithDetail(String type, @DoNotAutobox Object detail) {
        final CustomEventInit evt = CustomEventInit.create();
        evt.setDetail(detail);
        fireEvent(type, evt);
    }

    public void fireEvent(String type, CustomEventInit init) {
        fireEvent(type, new CustomEvent(type, init));
    }

    public void fireEvent(String type, Event e) {
        if (suppress) {
            JsLog.debug("Event suppressed", type, e);
            return;
        }
        if (map.has(e.type)) {
            final JsArray<EventFn> callbacks =
                Js.cast(JsArray.from((JsArrayLike<EventFn>) map.get(e.type)));
            callbacks.forEach((item, ind, all) -> {
                try {
                    item.onEvent(e);
                } catch (Throwable t) {
                    DomGlobal.console.error(logPrefix() + "User callback (", item, ") of type ",
                        type, " failed: ", t);
                    t.printStackTrace();
                }
                return true;
            });
        }
    }

    public boolean failureHandled(String failure) {
        if (failure != null) {
            if (hasListeners(EVENT_REQUEST_FAILED)) {
                final CustomEventInit event = CustomEventInit.create();
                event.setDetail(failure);
                fireEvent(EVENT_REQUEST_FAILED, event);
            } else {
                DomGlobal.console.error(logPrefix() + failure);
            }
            return true;
        }
        return false;
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
