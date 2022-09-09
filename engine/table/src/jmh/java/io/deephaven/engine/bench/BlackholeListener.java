package io.deephaven.engine.bench;

import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import org.openjdk.jmh.infra.Blackhole;

class BlackholeListener extends InstrumentedTableUpdateListener {

    private final Blackhole blackhole;
    int updates;
    Throwable e;

    public BlackholeListener(Blackhole blackhole) {
        super("Blackhole Listener");
        this.blackhole = blackhole;
    }

    @Override
    public void onUpdate(TableUpdate upstream) {
        blackhole.consume(upstream);
        ++updates;
    }

    @Override
    protected void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        e = originalException;
    }
}
