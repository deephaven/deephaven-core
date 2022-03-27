package io.deephaven.hotspot;

import com.google.auto.service.AutoService;
import io.deephaven.util.hotspot.HotSpot;
import sun.management.ManagementFactoryHelper;

@AutoService(HotSpot.class)
public class HotSpotImpl implements HotSpot {

    @Override
    public long getSafepointCount() {
        return ManagementFactoryHelper.getVMManagement().getSafepointCount();
    }

    @Override
    public long getTotalSafepointTimeMillis() {
        return ManagementFactoryHelper.getVMManagement().getTotalSafepointTime();
    }
}
