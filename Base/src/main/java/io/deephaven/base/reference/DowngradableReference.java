/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.reference;

import java.lang.ref.WeakReference;

/**
 * SimpleReference implementation that allows a downgrade from strongly reachable to weakly reachable.
 * 
 * @note This only applies (obviously) to this reference's relationship to the referent.
 * @param <T>
 */
public class DowngradableReference<T> extends WeakReference<T> implements SimpleReference<T> {

    /**
     * This hard reference exists purely to ensure reachability, until it's cleared.
     */
    private T hardReference;

    public DowngradableReference(T referent) {
        super(referent);
        hardReference = referent;
    }

    @Override
    public void clear() {
        downgrade();
        super.clear();
    }

    /**
     * Eliminate this object's hard reference to the referent. Converts the reachability enforced by this object from
     * hard to weak.
     */
    public void downgrade() {
        hardReference = null;
    }
}
