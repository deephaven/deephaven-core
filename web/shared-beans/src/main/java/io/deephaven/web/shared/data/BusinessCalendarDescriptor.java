package io.deephaven.web.shared.data;

import java.io.Serializable;

public class BusinessCalendarDescriptor implements Serializable {
    public enum DayOfWeek { SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY }

    private String name;
    private String timeZone;
    private DayOfWeek[] businessDays;
    private BusinessPeriod[] businessPeriods;
    private Holiday[] holidays;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public DayOfWeek[] getBusinessDays() {
        return businessDays;
    }

    public void setBusinessDays(DayOfWeek[] businessDays) {
        this.businessDays = businessDays;
    }

    public BusinessPeriod[] getBusinessPeriods() {
        return businessPeriods;
    }

    public void setBusinessPeriods(BusinessPeriod[] businessPeriods) {
        this.businessPeriods = businessPeriods;
    }

    public Holiday[] getHolidays() {
        return holidays;
    }

    public void setHolidays(Holiday[] holidays) {
        this.holidays = holidays;
    }
}
