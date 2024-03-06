//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.hotspot.impl;

import com.google.auto.service.AutoService;
import io.deephaven.hotspot.HotSpot;
import sun.management.ManagementFactoryHelper;
import sun.management.HotspotRuntimeMBean;

@AutoService(HotSpot.class)
public class HotSpotImpl implements HotSpot {
    private final HotspotRuntimeMBean hotspotRuntimeMBean;

    public HotSpotImpl() {
        hotspotRuntimeMBean = ManagementFactoryHelper.getHotspotRuntimeMBean();
    }

    @Override
    public long getSafepointCount() {
        return hotspotRuntimeMBean.getSafepointCount();
    }

    @Override
    public long getTotalSafepointTimeMillis() {
        return hotspotRuntimeMBean.getTotalSafepointTime();
    }

    @Override
    public long getSafepointSyncTimeMillis() {
        return hotspotRuntimeMBean.getSafepointSyncTime();
    }
}
