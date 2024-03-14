//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.clock;

import java.util.TimeZone;

public class TimeZones {
    public final static TimeZone TZ_GMT = TimeZone.getTimeZone("GMT");
    public final static TimeZone TZ_KOREA = TimeZone.getTimeZone("Asia/Seoul");
    public final static TimeZone TZ_CHICAGO = TimeZone.getTimeZone("America/Chicago");
    public final static TimeZone TZ_NEWYORK = TimeZone.getTimeZone("America/New_York");
    public final static TimeZone TZ_LONDON = TimeZone.getTimeZone("Europe/London");
    public final static TimeZone TZ_MUMBAI = TimeZone.getTimeZone("IST"); // India Standard Time
    public final static TimeZone TZ_BEIJING = TimeZone.getTimeZone("Asia/Shanghai");
    public final static TimeZone TZ_MOSCOW = TimeZone.getTimeZone("Europe/Moscow");
}
