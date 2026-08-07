package java.lang.ref;

public class WeakReference<T> extends Reference<T> {
    public WeakReference(T referent) {
        super(referent);
    }
    public WeakReference(T referent, ReferenceQueue<? super T> q) {
        super(referent, q);
    }
}
