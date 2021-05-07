package io.deephaven.web.shared.data;

import java.io.Serializable;

public class Holiday implements Serializable {
    private LocalDate date;
    private BusinessPeriod[] businessPeriods;

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }

    public BusinessPeriod[] getBusinessPeriods() {
        return businessPeriods;
    }

    public void setBusinessPeriods(BusinessPeriod[] businessPeriods) {
        this.businessPeriods = businessPeriods;
    }
}
