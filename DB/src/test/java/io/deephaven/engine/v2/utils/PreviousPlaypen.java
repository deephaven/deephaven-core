package io.deephaven.engine.v2.utils;

import io.deephaven.engine.v2.sources.LogicalClock;

public class PreviousPlaypen {
    public static void main(String[] args) {
        // LogicalClock.DEFAULT.startUpdateCycle();

        // final TrackingMutableRowSet i = RowSetFactoryImpl.INSTANCE.flat(100).convertToTracking();;
        // System.out.println(i);
        // System.out.println(i.getPrevRowSet());
        //
        // LogicalClock.DEFAULT.completeUpdateCycle();
        //
        // System.out.println(i);
        // System.out.println(i.getPrevRowSet());


        LogicalClock.DEFAULT.startUpdateCycle();

        final TrackingMutableRowSet i2 = RowSetFactoryImpl.INSTANCE.flat(200).convertToTracking();
        System.out.println(i2);
        System.out.println(i2.getPrevRowSet());

        i2.insert(500);

        System.out.println(i2);
        System.out.println(i2.getPrevRowSet());

        LogicalClock.DEFAULT.completeUpdateCycle();

        System.out.println(i2);
        System.out.println(i2.getPrevRowSet());

    }
}
