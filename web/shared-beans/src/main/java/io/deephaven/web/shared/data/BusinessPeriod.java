package io.deephaven.web.shared.data;

import java.io.Serializable;

public class BusinessPeriod implements Serializable {
    private String open;
    private String close;

    public String getOpen() {
        return open;
    }

    public void setOpen(String open) {
        this.open = open;
    }

    public String getClose() {
        return close;
    }

    public void setClose(String close) {
        this.close = close;
    }
}
