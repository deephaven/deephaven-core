package java.lang.ref;

import io.deephaven.web.client.fu.JsWeakRef;

public abstract class Reference<T> {

    private JsWeakRef<T> jsWeakRef;

    Reference(T referent) {
        this(referent, (ReferenceQueue) null);
    }

    Reference(T referent, ReferenceQueue<? super T> queue) {
        if (referent != null) {
            jsWeakRef = new JsWeakRef<>(referent);
        }
    }

    public T get() {
        if (jsWeakRef == null) {
            return null;
        }
        return jsWeakRef.deref();
    }

    public void clear() {
        if (jsWeakRef != null) {
            jsWeakRef = null;
        }
    }

    public boolean isEnqueued() {
        return false;
    }

    public boolean enqueue() {
        throw new IllegalStateException("never called when emulated");
    }

    protected Object clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

}
