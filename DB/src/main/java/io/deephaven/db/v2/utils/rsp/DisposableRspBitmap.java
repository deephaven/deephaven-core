package io.deephaven.db.v2.utils.rsp;

import io.deephaven.db.v2.utils.rsp.container.Container;

/**
 * "Disposable" version of {@link RspBitmap}, which allows other instances of {@link RspBitmap} to steal its containers.
 */
public class DisposableRspBitmap extends RspBitmap {

    public DisposableRspBitmap() {
    }

    public DisposableRspBitmap(long start, long end) {
        super(start, end);
    }

    @Override
    Container shareContainer(Container c) {
        return c;
    }
}
