package io.deephaven.web.shared.fu;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;

/**
 * A js-friendly Runnable FunctionalInterface
 */
@JsFunction
@FunctionalInterface
public interface JsRunnable {

    @JsOverlay
    static JsRunnable doNothing() {
        return RunnableHelper.DO_NOTHING;
    }

    void run();

    @JsOverlay
    default JsRunnable beforeMe(JsRunnable second) {
        return second.andThen(this);
    }

    @JsOverlay
    @SuppressWarnings("Convert2Lambda") // using anonymous type to work around annoying compiler bug
    default JsRunnable andThen(JsRunnable second) {
        JsRunnable self = JsRunnable.this;
        return new JsRunnable() {
            @Override
            public void run() {
                self.run();
                second.run();
            }
        };
    }

    @JsOverlay
    default <T> JsConsumer<T> asConsumer() {
        return ignore -> run();
    }
}


class RunnableHelper {
    // We put this here, in a non-js-interop type to avoid annoying compiler bugs.
    static final JsRunnable DO_NOTHING = () -> {
    };
}
