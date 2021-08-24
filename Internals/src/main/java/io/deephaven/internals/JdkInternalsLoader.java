package io.deephaven.internals;

public enum JdkInternalsLoader {
    ;

    public static JdkInternals getInstance() {
        return new SunMiscImpl();
    }
}
