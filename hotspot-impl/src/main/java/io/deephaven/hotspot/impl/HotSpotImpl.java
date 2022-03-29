package io.deephaven.hotspot.impl;

import com.google.auto.service.AutoService;
import io.deephaven.hotspot.HotSpot;
import sun.management.ManagementFactoryHelper;

@AutoService(HotSpot.class)
public class HotSpotImpl implements HotSpot {

    @Override
    public long getSafepointCount() {
        return ManagementFactoryHelper.getHotspotRuntimeMBean().getSafepointCount();
    }

    @Override
    public long getTotalSafepointTimeMillis() {
        return ManagementFactoryHelper.getHotspotRuntimeMBean().getTotalSafepointTime();
    }
}
