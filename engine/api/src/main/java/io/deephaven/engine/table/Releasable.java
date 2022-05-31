package io.deephaven.engine.table;

import javax.annotation.OverridingMethodsMustInvokeSuper;

/**
 * Interface for regions and regioned column sources that support releasing cached resources.
 */
public interface Releasable {

    /**
     * Release any resources held for caching purposes. Implementations need not guarantee that they are safe for normal
     * use concurrently with invocations of this method.
     */
    @OverridingMethodsMustInvokeSuper
    default void releaseCachedResources() {}
}
